from __future__ import annotations

import argparse
import uuid
import time
from typing import Any

import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values, register_uuid

from scrapling_demo.db_config import load_postgres_config


def _get_columns(cur, *, schema: str, table: str) -> set[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        """,
        (schema, table),
    )
    return {r[0] for r in cur.fetchall()}


def _find_table_schemas(cur, *, table: str) -> list[str]:
    cur.execute(
        """
        SELECT table_schema
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE' AND table_name = %s
        ORDER BY table_schema
        """,
        (table,),
    )
    return [r[0] for r in cur.fetchall()]


def _connect_with_retries(
    *,
    dsn: str,
    connect_timeout_s: int,
    retries: int,
    retry_delay_s: float,
) -> psycopg2.extensions.connection:
    last_err: Exception | None = None
    attempts = max(1, int(retries) + 1)
    for attempt in range(1, attempts + 1):
        try:
            return psycopg2.connect(dsn, connect_timeout=max(1, int(connect_timeout_s)))
        except psycopg2.OperationalError as e:
            last_err = e
            if attempt >= attempts:
                break
            sleep_s = float(retry_delay_s) * (2 ** (attempt - 1))
            print(
                f"Postgres connect failed (attempt {attempt}/{attempts}): {e}. "
                f"Retrying in {sleep_s:.1f}s..."
            )
            time.sleep(sleep_s)
    assert last_err is not None
    raise last_err


def _to_uuid(value: Any) -> uuid.UUID | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    if not s or s.lower() == "nan":
        return None
    try:
        return uuid.UUID(s)
    except Exception:
        return None


def _normalize_text(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    if not s or s.lower() == "nan":
        return None
    return s


def fill_text_video_from_excel(
    *,
    file_path: str,
    sheet_name: str | int | None,
    excel_storage_id_col: str,
    excel_text_col: str,
    schema: str | None,
    table: str,
    key_col: str,
    target_col: str,
    batch_size: int,
    only_null: bool,
    allow_empty_to_null: bool,
    dry_run: bool,
    connect_timeout_s: int = 10,
    connect_retries: int = 3,
    connect_retry_delay_s: float = 1.0,
) -> int:
    # pandas behavior:
    # - sheet_name=None -> dict of all sheets
    # - sheet_name=0 or "Sheet1" -> DataFrame
    excel_obj = pd.read_excel(file_path, sheet_name=sheet_name if sheet_name is not None else 0)
    if isinstance(excel_obj, dict):
        if not excel_obj:
            raise ValueError("Excel file contains no sheets.")
        df = next(iter(excel_obj.values()))
        print(f"Loaded Excel as multiple sheets; using first sheet with {len(df)} rows.")
    else:
        df = excel_obj

    required = {excel_storage_id_col, excel_text_col}
    if not required.issubset(set(df.columns)):
        raise ValueError(
            f"Excel must contain columns {sorted(required)}. Got: {list(df.columns)}"
        )

    df = df.dropna(subset=[excel_storage_id_col]).copy()
    df["_storage_id"] = df[excel_storage_id_col].map(_to_uuid)
    df["_text_video"] = df[excel_text_col].map(_normalize_text)
    df = df.dropna(subset=["_storage_id"]).copy()

    if allow_empty_to_null:
        # keep rows even when text is empty -> will set NULL
        pass
    else:
        df = df.dropna(subset=["_text_video"]).copy()

    # Remove duplicate storage_id (keep last occurrence)
    df = df.drop_duplicates(subset=["_storage_id"], keep="last").copy()

    if df.empty:
        print("No valid rows found in Excel. Nothing to update.")
        return 0

    records: list[tuple[uuid.UUID, str | None]] = list(
        df[["_storage_id", "_text_video"]].itertuples(index=False, name=None)
    )

    cfg = load_postgres_config()
    print(f"Connecting Postgres: {cfg.host}:{cfg.port}/{cfg.database}")

    conn = _connect_with_retries(
        dsn=cfg.dsn,
        connect_timeout_s=connect_timeout_s,
        retries=connect_retries,
        retry_delay_s=connect_retry_delay_s,
    )
    register_uuid(conn_or_curs=conn)
    cur = conn.cursor()
    try:
        effective_schema = (schema or "").strip() or getattr(cfg, "schema", "") or "public"
        effective_table = (table or "").strip()
        if not effective_table:
            raise ValueError("Target table is empty.")

        cols = _get_columns(cur, schema=effective_schema, table=effective_table)
        if not cols:
            candidates = _find_table_schemas(cur, table=effective_table)
            if len(candidates) == 1 and candidates[0] != effective_schema:
                effective_schema = candidates[0]
                cols = _get_columns(cur, schema=effective_schema, table=effective_table)
            elif candidates:
                raise RuntimeError(
                    f"Table {effective_schema}.{effective_table} not found. "
                    f"Found table '{effective_table}' in schemas: {candidates}. "
                    f"Pass the right schema via --schema."
                )
            else:
                raise RuntimeError(
                    f"Table {effective_schema}.{effective_table} not found. "
                    f"No table named '{effective_table}' exists in any schema."
                )

        missing_cols = [c for c in (key_col, target_col) if c not in cols]
        if missing_cols:
            raise RuntimeError(
                f"Table {effective_schema}.{effective_table} missing columns: {missing_cols}. "
                f"Existing: {sorted(cols)}"
            )

        update_query = sql.SQL(
            """
            UPDATE {table} AS sv
            SET {target_col} = data.{target_col}
            FROM (VALUES %s) AS data({key_col}, {target_col})
            WHERE sv.{key_col} = data.{key_col}
            """
        ).format(
            table=sql.Identifier(effective_schema, effective_table),
            key_col=sql.Identifier(key_col),
            target_col=sql.Identifier(target_col),
        )
        if only_null:
            update_query += sql.SQL(" AND sv.{target_col} IS NULL").format(
                target_col=sql.Identifier(target_col)
            )
        update_query_str = update_query.as_string(cur)

        print(f"Rows prepared from Excel: {len(records)} (batch_size={batch_size})")
        if dry_run:
            print("Dry-run enabled: no database writes performed.")
            return 0

        total_updated = 0
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            execute_values(cur, update_query_str, batch, page_size=len(batch))
            conn.commit()
            total_updated += int(cur.rowcount or 0)
            print(
                f"Batch {i // batch_size + 1}: input={len(batch)} updated={int(cur.rowcount or 0)}"
            )

        print(f"Done. Total updated rows: {total_updated}")
        return total_updated
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def main() -> int:
    p = argparse.ArgumentParser(
        description="Fill storage_video.text_video from video_content.xlsx by storage_id."
    )
    p.add_argument(
        "--file",
        default="video_content.xlsx",
        help="Excel path (default: video_content.xlsx)",
    )
    p.add_argument(
        "--sheet",
        default=None,
        help="Sheet name or index (default: first sheet). Example: --sheet 0 or --sheet Sheet1",
    )
    p.add_argument(
        "--excel-storage-id-col",
        default="storage_id",
        help="Excel column name for storage_id (default: storage_id)",
    )
    p.add_argument(
        "--excel-text-col",
        default="text_video",
        help="Excel column name for text_video (default: text_video)",
    )
    p.add_argument(
        "--schema",
        default=None,
        help="Postgres schema name (default: from db_config.py, fallback: public). Example: --schema n8n",
    )
    p.add_argument("--table", default="storage_video", help="Target table (default: storage_video)")
    p.add_argument(
        "--key-col",
        default="storage_id",
        help="Key column in table used to match (default: storage_id)",
    )
    p.add_argument(
        "--target-col",
        default="text_video",
        help="Target column to fill (default: text_video)",
    )
    p.add_argument("--batch-size", type=int, default=500, help="Batch size (default: 500)")
    p.add_argument(
        "--connect-timeout",
        type=int,
        default=10,
        help="Postgres connect timeout seconds (default: 10).",
    )
    p.add_argument(
        "--connect-retries",
        type=int,
        default=3,
        help="Retries after initial connect attempt (default: 3).",
    )
    p.add_argument(
        "--connect-retry-delay",
        type=float,
        default=1.0,
        help="Initial retry delay seconds; doubles each retry (default: 1.0).",
    )
    p.add_argument(
        "--only-null",
        action="store_true",
        help="Only update rows where target column is NULL (default: off).",
    )
    p.add_argument(
        "--allow-empty-to-null",
        action="store_true",
        help="Allow empty/NaN Excel text to set NULL in DB (default: off; empty rows skipped).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Read/validate Excel but do not write to DB.",
    )

    args = p.parse_args()
    sheet: str | int | None = args.sheet
    if isinstance(sheet, str) and sheet.strip() != "":
        s = sheet.strip()
        if s.isdigit():
            sheet = int(s)
        else:
            sheet = s
    else:
        sheet = 0

    fill_text_video_from_excel(
        file_path=args.file,
        sheet_name=sheet,
        excel_storage_id_col=args.excel_storage_id_col,
        excel_text_col=args.excel_text_col,
        schema=args.schema.strip() if isinstance(args.schema, str) else None,
        table=(args.table or "").strip(),
        key_col=args.key_col,
        target_col=args.target_col,
        batch_size=max(1, int(args.batch_size)),
        only_null=bool(args.only_null),
        allow_empty_to_null=bool(args.allow_empty_to_null),
        dry_run=bool(args.dry_run),
        connect_timeout_s=max(1, int(args.connect_timeout)),
        connect_retries=max(0, int(args.connect_retries)),
        connect_retry_delay_s=max(0.1, float(args.connect_retry_delay)),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())