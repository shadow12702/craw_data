try:
    # When running from repo root: `python scrapling_demo/test.py`
    from scrapling_demo.db_config import load_postgres_config
except Exception:
    # When running inside folder: `python test.py`
    from db_config import load_postgres_config

import uuid


def _require_tables(cur, *, schema: str) -> None:
    cur.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name IN ('storage_data', 'storage_image', 'storage_video')
        ORDER BY table_name;
        """,
        (schema,),
    )
    tables = [r[0] for r in cur.fetchall()]
    missing = sorted(set(["storage_data", "storage_image", "storage_video"]) - set(tables))
    if missing:
        raise SystemExit(f"Thiếu bảng trong schema {schema!r}: {missing}. Hãy CREATE TABLE trước rồi test lại.")


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
            # Ensure we can resolve unqualified table names in non-public schemas
            cur.execute('SET search_path TO "{}";'.format(str(schema).replace('"', '""')))
            cur.execute("SELECT 1, current_database(), current_user, version();")
            print("OK:", cur.fetchone())
            cur.execute("SHOW server_version;")
            print("server_version:", cur.fetchone()[0])

            # Match crawler assumptions: tables exist
            _require_tables(cur, schema=schema)
            print("Tables OK: storage_data, storage_image, storage_video")

            # Smoke test write path (insert+delete) using deterministic UUID from URL
            test_url = "https://benhvienanbinh.vn/__cursor_db_smoke_test__"
            doc_id = uuid.uuid5(uuid.NAMESPACE_URL, test_url)
            img_id = uuid.uuid5(uuid.NAMESPACE_URL, f"{doc_id}|image|{test_url}/img.png")
            vid_id = uuid.uuid5(uuid.NAMESPACE_URL, f"{doc_id}|video|{test_url}/vid.mp4")

            cur.execute(
                """
                INSERT INTO storage_data (id, title, content, link)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE
                  SET title = EXCLUDED.title,
                      content = EXCLUDED.content,
                      link = EXCLUDED.link;
                """,
                (doc_id, "DB Smoke Test", "Hello", test_url),
            )
            cur.execute(
                """
                INSERT INTO storage_image (id, storage_id, image_url)
                VALUES (%s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
                """,
                (img_id, doc_id, f"{test_url}/img.png"),
            )
            cur.execute(
                """
                INSERT INTO storage_video (id, storage_id, video_url)
                VALUES (%s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
                """,
                (vid_id, doc_id, f"{test_url}/vid.mp4"),
            )

            # Clean up so test doesn't leave data behind
            cur.execute("DELETE FROM storage_data WHERE id = %s;", (doc_id,))
            conn.commit()
            print("Write path OK (insert/delete)")

if __name__ == "__main__":
    main()