from __future__ import annotations

import argparse
import json
import os
import uuid
from datetime import datetime
from typing import Any, Iterable, Sequence

try:
    import psycopg2
    from psycopg2 import sql
    from psycopg2.extras import execute_values
except ImportError as e:  # pragma: no cover
    raise ImportError("Cần cài: pip install psycopg2-binary") from e

from scrapling_demo.db_config import load_postgres_config


DATASET_COLUMNS: tuple[str, ...] = (
    "symptom_text",
    "khoa_chinh",
    "khoa_phu",
    "priority_level",
    "priority_score",
    "emergency_flag",
    "first_aid_instruction",
    "age_group",
    "gender_bias",
    "symptom_group",
)

REQUIRED_COLUMNS: tuple[tuple[str, str], ...] = (
    ("symptom_text", "TEXT"),
    ("khoa_chinh", "TEXT"),
    ("khoa_phu", "TEXT"),
    ("priority_level", "TEXT"),
    ("priority_score", "INTEGER"),
    ("emergency_flag", "INTEGER"),
    ("first_aid_instruction", "TEXT"),
    ("age_group", "TEXT"),
    ("gender_bias", "TEXT"),
    ("symptom_group", "TEXT"),
    ("created_at", "TIMESTAMP"),
)


def _ensure_schema(cur, table: str) -> None:
    """
    Tạo bảng nếu chưa có, và đảm bảo các cột tối thiểu tồn tại.
    An toàn khi chạy nhiều lần.
    """
    cur.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {} (
                id UUID PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                symptom_text TEXT,
                khoa_chinh TEXT,
                khoa_phu TEXT,
                priority_level TEXT,
                priority_score INTEGER,
                emergency_flag INTEGER,
                first_aid_instruction TEXT,
                age_group TEXT,
                gender_bias TEXT,
                symptom_group TEXT
            );
            """
        ).format(sql.Identifier(table))
    )
    for col, col_type in REQUIRED_COLUMNS:
        cur.execute(
            sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {};").format(
                sql.Identifier(table),
                sql.Identifier(col),
                sql.SQL(col_type),
            )
        )


def _get_table_columns(cur, table: str) -> dict[str, dict[str, Any]]:
    cur.execute(
        """
        SELECT column_name, data_type, udt_name, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        """,
        (table,),
    )
    return {
        name: {
            "data_type": data_type,
            "udt_name": udt_name,
            "is_nullable": nullable,
            "default": default,
        }
        for name, data_type, udt_name, nullable, default in cur.fetchall()
    }


def _make_insert_sql(table: str, columns: Sequence[str]) -> sql.Composed:
    cols = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
    return sql.SQL("INSERT INTO {} ({}) VALUES %s").format(sql.Identifier(table), cols)


def _make_upsert_sql(table: str, columns: Sequence[str], conflict_col: str) -> sql.Composed:
    base = _make_insert_sql(table, columns)
    update_cols = [c for c in columns if c != conflict_col]
    if not update_cols:
        return base
    assignments = sql.SQL(", ").join(
        sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c)) for c in update_cols
    )
    return sql.SQL("{} ON CONFLICT ({}) DO UPDATE SET {}").format(
        base,
        sql.Identifier(conflict_col),
        assignments,
    )


def _to_bool(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        return bool(v)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in {"1", "true", "t", "yes", "y"}:
            return True
        if s in {"0", "false", "f", "no", "n"}:
            return False
    return v


def _to_int(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, bool):
        return 1 if v else 0
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    if isinstance(v, str):
        s = v.strip()
        if s == "":
            return None
        try:
            return int(float(s))
        except Exception:
            return None
    return None


def _stable_uuid(item: dict[str, Any]) -> uuid.UUID:
    def _norm(x: Any) -> str:
        if x is None:
            return ""
        return str(x).strip()

    key = "|".join(
        [
            _norm(item.get("symptom_text")),
            _norm(item.get("khoa_chinh")),
            _norm(item.get("khoa_phu")),
            _norm(item.get("age_group")),
            _norm(item.get("gender_bias")),
            _norm(item.get("symptom_group")),
        ]
    )
    return uuid.uuid5(uuid.NAMESPACE_URL, key)


def _coerce_for_column(value: Any, meta: dict[str, Any]) -> Any:
    dt = (meta.get("data_type") or "").lower()
    udt = (meta.get("udt_name") or "").lower()

    if dt in {"uuid"} or udt == "uuid":
        if value is None:
            return None
        if isinstance(value, uuid.UUID):
            return str(value)
        return str(uuid.UUID(str(value)))

    if dt in {"integer", "smallint", "bigint"}:
        return _to_int(value)

    if dt == "boolean":
        b = _to_bool(value)
        return bool(b) if b is not None else None

    if dt.startswith("timestamp"):
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        # Let Postgres attempt to parse common timestamp strings
        return str(value)

    # text/varchar/json/etc: just pass through as str (keep None)
    if value is None:
        return None
    return str(value)


def _iter_rows(
    data: Sequence[dict[str, Any]],
    insert_columns: Sequence[str],
    table_cols: dict[str, dict[str, Any]],
) -> Iterable[tuple[Any, ...]]:
    for item in data:
        symptom_text = item.get("symptom_text")
        value_by_col: dict[str, Any] = {
            "id": _stable_uuid(item),
            "created_at": item.get("created_at"),
            "symptom_text": symptom_text,
            # Common alternate schemas
            "text": symptom_text,
            "name": symptom_text,
            "khoa_chinh": item.get("khoa_chinh"),
            "khoa_phu": item.get("khoa_phu"),
            "priority_level": item.get("priority_level"),
            "priority_score": item.get("priority_score"),
            "emergency_flag": item.get("emergency_flag"),
            "first_aid_instruction": item.get("first_aid_instruction"),
            "age_group": item.get("age_group"),
            "gender_bias": item.get("gender_bias"),
            "symptom_group": item.get("symptom_group"),
            # Some existing schemas require these (NOT NULL w/o default)
            "title": symptom_text,
            "content": (
                symptom_text
                if symptom_text is not None
                else json.dumps(item, ensure_ascii=False)
            ),
        }
        out: list[Any] = []
        for c in insert_columns:
            v = value_by_col.get(c)
            meta = table_cols.get(c)
            if meta:
                v = _coerce_for_column(v, meta)
            out.append(v)
        yield tuple(out)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Import Dataset_Trieu_Chung JSON into Postgres."
    )
    parser.add_argument(
        "--json-path",
        default=None,
        help="Path to JSON file (default: auto pick Dataset_Trieu_Chung.json if exists).",
    )
    parser.add_argument(
        "--table",
        default="symptoms",
        help="Target table name (default: symptoms).",
    )
    parser.add_argument(
        "--allow-symptom-dataset",
        action="store_true",
        help="Safety override: allow writing to symptom_dataset (default: blocked).",
    )
    parser.add_argument(
        "--batch-size", type=int, default=1000, help="Insert batch size."
    )
    args = parser.parse_args()

    if args.table == "symptom_dataset" and not args.allow_symptom_dataset:
        raise SystemExit(
            "Blocked: script không được ghi vào table 'symptom_dataset'. "
            "Hãy dùng '--table symptoms'. "
            "Nếu bạn thật sự muốn ghi vào symptom_dataset, chạy thêm '--allow-symptom-dataset'."
        )

    if args.json_path:
        json_path = args.json_path
    else:
        base = os.path.dirname(__file__)
        cand1 = os.path.join(base, "Dataset_Trieu_Chung.json")
        cand2 = os.path.join(base, "Dataset_Trieu_Chung 1.json")
        json_path = cand1 if os.path.isfile(cand1) else cand2

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("JSON phải là list các record.")

    cfg = load_postgres_config()
    print(
        f"Connecting Postgres: {cfg.host}:{cfg.port}/{cfg.database} (sslmode={cfg.sslmode})"
    )

    conn = psycopg2.connect(cfg.dsn)
    cur = conn.cursor()
    try:
        _ensure_schema(cur, args.table)
        conn.commit()

        table_cols = _get_table_columns(cur, args.table)
        # Only insert columns that actually exist in the table.
        # If table has UUID id, we always include it so the import is idempotent (upsert).
        insert_cols: list[str] = [c for c, _ in REQUIRED_COLUMNS if c in table_cols]
        if "id" in table_cols:
            id_meta = table_cols["id"]
            if (id_meta.get("data_type") or "").lower() == "uuid" or (id_meta.get("udt_name") or "").lower() == "uuid":
                if "id" not in insert_cols:
                    insert_cols.insert(0, "id")

        # If table already exists with required NOT NULL columns (no default), include them in insert.
        for col_name, meta in table_cols.items():
            if meta.get("is_nullable") == "NO" and meta.get("default") is None and col_name not in insert_cols:
                # don't force-insert typical auto columns
                if col_name in {"updated_at"}:
                    continue
                insert_cols.append(col_name)

        # Prefer upsert when we can provide stable UUID ids.
        if "id" in insert_cols:
            insert_sql = _make_upsert_sql(args.table, insert_cols, "id")
        else:
            insert_sql = _make_insert_sql(args.table, insert_cols)

        rows = list(_iter_rows(data, insert_cols, table_cols))
        # When using ON CONFLICT (id), all proposed rows in a single statement must have unique ids.
        if "id" in insert_cols:
            id_idx = insert_cols.index("id")
            dedup: dict[Any, tuple[Any, ...]] = {}
            for r in rows:
                dedup[r[id_idx]] = r  # keep last occurrence
            if len(dedup) != len(rows):
                print(f"Dedup by id: {len(rows)} -> {len(dedup)}")
            rows = list(dedup.values())

        print(f"Total records: {len(rows)}")

        execute_values(cur, insert_sql, rows, page_size=max(1, int(args.batch_size)))
        conn.commit()

        # Quick verification (numbers only, safe for console encodings)
        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(args.table)))
        total_in_table = cur.fetchone()[0]
        if "symptom_text" in table_cols:
            cur.execute(
                sql.SQL("SELECT COUNT(*) FROM {} WHERE symptom_text IS NOT NULL").format(
                    sql.Identifier(args.table)
                )
            )
            non_null_symptom_text = cur.fetchone()[0]
        else:
            non_null_symptom_text = None
    finally:
        cur.close()
        conn.close()

    print("Import completed successfully.")
    print(f"Rows in {args.table}: {total_in_table}")
    if non_null_symptom_text is not None:
        print(f"Rows with symptom_text not null: {non_null_symptom_text}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
