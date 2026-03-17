try:
    # When running from repo root: `python scrapling_demo/test.py`
    from scrapling_demo.db_config import REQUIRED_TABLES, load_postgres_config
except Exception:
    # When running inside folder: `python test.py`
    from db_config import REQUIRED_TABLES, load_postgres_config


def _check_tables(cur, *, schema: str) -> list[str]:
    cur.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name = ANY(%s)
        ORDER BY table_name;
        """,
        (schema, list(REQUIRED_TABLES)),
    )
    tables = [r[0] for r in cur.fetchall()]
    return sorted(set(REQUIRED_TABLES) - set(tables))


def main():
    cfg = load_postgres_config()
    dsn = cfg.dsn
    schema = getattr(cfg, "schema", None) or "public"
    try:
        import psycopg
    except ImportError:
        raise SystemExit("Thiếu psycopg. Cài: pip install psycopg[binary]")

    print(f"Connecting to: host={cfg.host} port={cfg.port} db={cfg.database} user={cfg.username}")
    print("DSN:", cfg.dsn)

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1, current_database(), current_user, version();")
            print("OK:", cur.fetchone())
            cur.execute("SHOW server_version;")
            print("server_version:", cur.fetchone()[0])

            missing = _check_tables(cur, schema=schema)
            if missing:
                print(f"Không có bảng trong schema {schema!r}: {missing}")
                return

            print("Tables OK:", ", ".join(REQUIRED_TABLES))

if __name__ == "__main__":
    main()