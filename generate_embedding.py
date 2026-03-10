from __future__ import annotations

import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import os
from typing import Any, Iterable

import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json, execute_batch
import requests
from requests import RequestException

from scrapling_demo.db_config import load_postgres_config


def _normalize_ollama_host(host: str) -> str:
    h = (host or "").strip()
    if not h:
        return ""
    if not h.lower().startswith(("http://", "https://")):
        h = "http://" + h
    return h.rstrip("/")


def _vector_to_pgvector_text(vec: list[float], dim: int) -> str:
    # pgvector text format: [1,2,3]
    v = [float(x) for x in (vec or [])]
    if len(v) != dim:
        if len(v) > dim:
            v = v[:dim]
        else:
            v = v + [0.0] * (dim - len(v))
    return "[" + ",".join(f"{float(x):.10g}" for x in v) + "]"


def _ollama_embed(base_url: str, model: str, text: str, timeout_s: int) -> list[float]:
    base = base_url.rstrip("/")
    try:
        # Prefer newer /api/embed
        r = requests.post(
            f"{base}/api/embed",
            json={"model": model, "input": [text]},
            timeout=timeout_s,
        )
        r.raise_for_status()
        data = r.json()
        embs = data.get("embeddings")
        if isinstance(embs, list) and embs and isinstance(embs[0], list):
            emb = embs[0]
        else:
            # Fall back to older /api/embeddings
            r2 = requests.post(
                f"{base}/api/embeddings",
                json={"model": model, "prompt": text},
                timeout=timeout_s,
            )
            r2.raise_for_status()
            data2 = r2.json()
            emb = data2.get("embedding")
    except RequestException as e:
        raise RuntimeError(
            f"Không gọi được Ollama embeddings API tại {base_url!r}: {e}"
        ) from e
    if not isinstance(emb, list) or not emb:
        raise ValueError("Ollama response missing embedding(s) field")
    return [float(x) for x in emb]


def _get_columns(cur, table: str) -> dict[str, dict[str, Any]]:
    cur.execute(
        """
        SELECT column_name, data_type, udt_name, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        """,
        (table,),
    )
    return {
        name: {"data_type": dt, "udt_name": udt, "is_nullable": nullable}
        for name, dt, udt, nullable in cur.fetchall()
    }


def _pick_text_column(cols: dict[str, dict[str, Any]]) -> str:
    for c in ("symptom_text", "content", "title", "text"):
        if c in cols:
            return c
    raise SystemExit(
        "Không tìm thấy cột text để embed (thử: symptom_text/content/title)."
    )


def _ensure_embedding_column(
    cur,
    table: str,
    cols: dict[str, dict[str, Any]],
    embedding_col: str,
    metadata_col: str,
    dim: int,
) -> dict[str, Any]:
    # Ensure pgvector exists (type "vector").
    try:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
    except Exception as e:
        raise SystemExit(
            "Không tạo/không có pgvector extension. "
            "Hãy cài pgvector và chạy: CREATE EXTENSION vector; "
            f"(error={e})"
        ) from e

    if embedding_col not in cols:
        cur.execute(
            sql.SQL("ALTER TABLE {} ADD COLUMN {} VECTOR(%s);").format(
                sql.Identifier(table),
                sql.Identifier(embedding_col),
            ),
            (dim,),
        )

    if metadata_col and metadata_col not in cols:
        cur.execute(
            sql.SQL("ALTER TABLE {} ADD COLUMN {} JSONB;").format(
                sql.Identifier(table),
                sql.Identifier(metadata_col),
            )
        )

    cols = _get_columns(cur, table)
    return cols[embedding_col]


def _iter_pending_rows(
    cur, table: str, id_col: str, text_col: str, embedding_col: str, limit: int
):
    cur.execute(
        sql.SQL("SELECT {}, {} FROM {} WHERE {} IS NULL LIMIT %s").format(
            sql.Identifier(id_col),
            sql.Identifier(text_col),
            sql.Identifier(table),
            sql.Identifier(embedding_col),
        ),
        (limit,),
    )
    yield from cur.fetchall()


def _update_embeddings(
    cur,
    table: str,
    id_col: str,
    embedding_col: str,
    metadata_col: str | None,
    embedding_meta: dict[str, Any],
    updates: Iterable[tuple[Any, list[float]]],
    metadata: dict[str, Any],
    dim: int,
) -> None:
    # Handle both pgvector (udt_name=vector) and arrays (FLOAT8[]).
    if embedding_meta.get("udt_name") == "vector":
        if metadata_col:
            params = [
                (_vector_to_pgvector_text(vec, dim), Json(metadata), row_id)
                for row_id, vec in updates
            ]
            q = sql.SQL("UPDATE {} SET {} = %s::vector, {} = %s WHERE {} = %s").format(
                sql.Identifier(table),
                sql.Identifier(embedding_col),
                sql.Identifier(metadata_col),
                sql.Identifier(id_col),
            )
        else:
            params = [(_vector_to_pgvector_text(vec, dim), row_id) for row_id, vec in updates]
            q = sql.SQL("UPDATE {} SET {} = %s::vector WHERE {} = %s").format(
                sql.Identifier(table),
                sql.Identifier(embedding_col),
                sql.Identifier(id_col),
            )
        execute_batch(cur, q, params, page_size=500)
        return

    if metadata_col:
        q = sql.SQL("UPDATE {} SET {} = %s, {} = %s WHERE {} = %s").format(
            sql.Identifier(table),
            sql.Identifier(embedding_col),
            sql.Identifier(metadata_col),
            sql.Identifier(id_col),
        )
        params = [(vec, Json(metadata), row_id) for row_id, vec in updates]
    else:
        q = sql.SQL("UPDATE {} SET {} = %s WHERE {} = %s").format(
            sql.Identifier(table),
            sql.Identifier(embedding_col),
            sql.Identifier(id_col),
        )
        params = [(vec, row_id) for row_id, vec in updates]
    execute_batch(cur, q, params, page_size=500)


def main() -> int:
    p = argparse.ArgumentParser(
        description="Generate embeddings for Postgres table using Ollama embeddings API."
    )
    p.add_argument(
        "--table",
        default="symptoms",
        help="Target table (default: symptoms).",
    )
    p.add_argument(
        "--allow-symptom-dataset",
        action="store_true",
        help="Safety override: allow writing to symptom_dataset (default: blocked).",
    )
    p.add_argument("--id-col", default="id", help="ID column (default: id).")
    p.add_argument(
        "--embedding-col",
        default="embedding",
        help="Embedding column (default: embedding).",
    )
    p.add_argument(
        "--metadata-col",
        default="metadata",
        help="Metadata column (default: metadata). Set empty to disable.",
    )
    p.add_argument("--dim", type=int, default=768, help="Embedding dimension (default: 768).")
    p.add_argument("--batch-size", type=int, default=100, help="Rows per batch.")
    p.add_argument(
        "--ollama-url",
        default=_normalize_ollama_host(os.getenv("OLLAMA_HOST") or "192.168.1.66:11434"),
        help="Ollama base URL (default from OLLAMA_HOST like crawl4ai script).",
    )
    p.add_argument(
        "--model",
        default=(os.getenv("OLLAMA_EMBED_MODEL") or "").strip() or "nomic-embed-text:latest",
        help="Embedding model name (default from OLLAMA_EMBED_MODEL).",
    )
    p.add_argument("--timeout", type=int, default=60, help="HTTP timeout seconds.")
    p.add_argument(
        "--max-workers", type=int, default=4, help="Parallel embedding workers."
    )
    p.add_argument(
        "--on-error",
        choices=("stop", "skip"),
        default="stop",
        help="On embedding error: stop (default) or skip rows.",
    )
    args = p.parse_args()

    if args.table == "symptom_dataset" and not args.allow_symptom_dataset:
        raise SystemExit(
            "Blocked: script không được ghi vào table 'symptom_dataset'. "
            "Hãy dùng '--table symptoms'. "
            "Nếu bạn thật sự muốn ghi vào symptom_dataset, chạy thêm '--allow-symptom-dataset'."
        )

    cfg = load_postgres_config()
    print(
        f"Connecting Postgres: {cfg.host}:{cfg.port}/{cfg.database} (sslmode={cfg.sslmode})"
    )

    conn = psycopg2.connect(cfg.dsn)
    cur = conn.cursor()
    try:
        cols = _get_columns(cur, args.table)
        if not cols:
            raise SystemExit(f"Không tìm thấy bảng public.{args.table}.")

        text_col = _pick_text_column(cols)
        metadata_col = (args.metadata_col or "").strip() or None
        embedding_meta = _ensure_embedding_column(
            cur,
            args.table,
            cols,
            args.embedding_col,
            metadata_col or "",
            int(args.dim),
        )
        conn.commit()

        meta_payload = {
            "provider": "ollama",
            "model": args.model,
            "dim": int(args.dim),
            "source_text_col": text_col,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

        while True:
            rows = list(
                _iter_pending_rows(
                    cur,
                    args.table,
                    args.id_col,
                    text_col,
                    args.embedding_col,
                    args.batch_size,
                )
            )
            if not rows:
                break

            print(
                f"Processing batch: {len(rows)} rows (text_col={text_col}, embedding_col={args.embedding_col})"
            )

            updates: list[tuple[Any, list[float]]] = []
            with ThreadPoolExecutor(max_workers=max(1, args.max_workers)) as ex:
                fut_to_id = {}
                for row_id, text in rows:
                    if text is None or (isinstance(text, str) and not text.strip()):
                        continue
                    fut = ex.submit(
                        _ollama_embed,
                        args.ollama_url,
                        args.model,
                        str(text),
                        args.timeout,
                    )
                    fut_to_id[fut] = row_id

                for fut in as_completed(fut_to_id):
                    row_id = fut_to_id[fut]
                    try:
                        vec = fut.result()
                        updates.append((row_id, vec))
                    except Exception as e:
                        msg = (
                            f"Embedding failed for id={row_id!r}: {e}\n"
                            f"- Hãy đảm bảo Ollama đang chạy và có model '{args.model}'.\n"
                            f"- Ví dụ: `ollama serve` và `ollama pull {args.model}`\n"
                            f"- Hoặc đổi endpoint bằng `--ollama-url http://<host>:11434`."
                        )
                        if args.on_error == "skip":
                            print(msg)
                            continue
                        raise SystemExit(msg) from e

            if not updates:
                print("No valid text rows in this batch; stopping.")
                break

            _update_embeddings(
                cur,
                args.table,
                args.id_col,
                args.embedding_col,
                metadata_col,
                embedding_meta,
                updates,
                meta_payload,
                int(args.dim),
            )
            conn.commit()

    finally:
        cur.close()
        conn.close()

    print("All embeddings generated.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
