from dataclasses import dataclass
from urllib.parse import quote


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    database: str
    username: str
    password: str
    sslmode: str = "prefer"
    schema: str = "n8n"

    @property
    def dsn(self) -> str:
        # psycopg/asyncpg compatible DSN
        user = quote(self.username, safe="")
        pwd = quote(self.password, safe="")
        db = quote(self.database, safe="")
        return f"postgresql://{user}:{pwd}@{self.host}:{self.port}/{db}?sslmode={self.sslmode}"


POSTGRES_CONFIG = PostgresConfig(
    # NOTE:
    # - Inside Docker network: host is usually "pgvector"
    # - From Windows host (outside Docker): use Docker host IP or localhost (because ports: "5432:5432")
    host="192.168.100.68",
    port=5432,
    database="n8n",
    username="n8n",
    password="123456",
    sslmode="prefer",
    schema="n8n",
)


TABLE_STORAGE_DATA = "storage_data"
TABLE_STORAGE_VIDEO = "storage_video"
TABLE_STORAGE_IMAGE = "storage_image"
TABLE_PDF = "pdf_documents"

REQUIRED_TABLES = (
    TABLE_STORAGE_DATA,
    TABLE_STORAGE_IMAGE,
    TABLE_STORAGE_VIDEO,
    TABLE_PDF,
)

STORAGE_DATA_COLUMNS = ("id", "title", "content", "link", "created_at")
STORAGE_VIDEO_COLUMNS = ("id", "storage_id", "video_url")
STORAGE_IMAGE_COLUMNS = ("id", "storage_id", "image_url")
PDF_COLUMNS = ("id", "file_name", "file_url", "content", "created_at")
TABLE_COLUMNS = {
    TABLE_STORAGE_DATA: STORAGE_DATA_COLUMNS,
    TABLE_STORAGE_VIDEO: STORAGE_VIDEO_COLUMNS,
    TABLE_STORAGE_IMAGE: STORAGE_IMAGE_COLUMNS,
}


def load_postgres_config() -> PostgresConfig:
    """Static config only (no .env, no env overrides)."""
    return POSTGRES_CONFIG


# -*- coding: utf-8 -*-
"""
Cấu hình kết nối PostgreSQL.
Server: http://192.168.1.66:5050 (giao diện web).
Kết nối trực tiếp DB dùng host/port bên dưới (PostgreSQL thường dùng port 5432).
"""

def _quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def qualified_table_name(table_name: str, schema: str | None = None) -> str:
    schema_name = schema or POSTGRES_CONFIG.schema or "public"
    return f'{_quote_ident(schema_name)}.{_quote_ident(table_name)}'


def _require_confirm(confirm: bool, action: str) -> None:
    if not confirm:
        raise RuntimeError(
            f"Refusing to {action} without explicit confirmation. Pass confirm=True or use --confirm."
        )


def build_create_tables_sql(schema: str) -> str:
    schema_ident = _quote_ident(schema or "public")
    storage_data = qualified_table_name(TABLE_STORAGE_DATA, schema)
    storage_video = qualified_table_name(TABLE_STORAGE_VIDEO, schema)
    storage_image = qualified_table_name(TABLE_STORAGE_IMAGE, schema)
    return f"""
CREATE SCHEMA IF NOT EXISTS {schema_ident};

CREATE TABLE IF NOT EXISTS {storage_data} (
    id UUID PRIMARY KEY,
    title VARCHAR(255),
    content TEXT,
    link TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS {storage_video} (
    id UUID PRIMARY KEY,
    storage_id UUID REFERENCES {storage_data}(id) ON DELETE CASCADE,
    video_url TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS {storage_image} (
    id UUID PRIMARY KEY,
    storage_id UUID REFERENCES {storage_data}(id) ON DELETE CASCADE,
    image_url TEXT NOT NULL
);
"""


def migrate_url_columns_to_text(*, confirm: bool = False) -> None:
    """
    Nâng giới hạn URL để tránh bị cắt (URL dài có token sẽ hỏng nếu bị truncate).
    An toàn khi chạy nhiều lần.
    """
    _require_confirm(confirm, "migrate URL columns to TEXT")
    schema = getattr(POSTGRES_CONFIG, "schema", None) or "public"
    storage_data = qualified_table_name(TABLE_STORAGE_DATA, schema)
    storage_video = qualified_table_name(TABLE_STORAGE_VIDEO, schema)
    storage_image = qualified_table_name(TABLE_STORAGE_IMAGE, schema)
    conn = get_connection()
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(f"ALTER TABLE {storage_data} ALTER COLUMN link TYPE TEXT;")
        cur.execute(f"ALTER TABLE {storage_video} ALTER COLUMN video_url TYPE TEXT;")
        cur.execute(f"ALTER TABLE {storage_image} ALTER COLUMN image_url TYPE TEXT;")
        conn.commit()
        print(f"Đã migrate link/video_url/image_url sang TEXT trong schema {schema}.")
    finally:
        if cur:
            cur.close()
        conn.close()


def migrate_youtube_embed_urls_to_watch(*, confirm: bool = False) -> None:
    """
    Chuyển URL YouTube dạng /embed/... trong storage_video sang dạng watch/playlist,
    đồng thời decode '&amp;' -> '&' và '//' -> 'https://'.
    """
    _require_confirm(confirm, "migrate YouTube embed URLs")
    schema = getattr(POSTGRES_CONFIG, "schema", None) or "public"
    storage_video = qualified_table_name(TABLE_STORAGE_VIDEO, schema)
    conn = get_connection()
    cur = None
    try:
        cur = conn.cursor()

        # Decode HTML entity first
        cur.execute(
            f"""
            UPDATE {storage_video}
            SET video_url = REPLACE(video_url, '&amp;', '&')
            WHERE video_url LIKE '%&amp;%';
            """
        )

        # Scheme-relative -> https
        cur.execute(
            f"""
            UPDATE {storage_video}
            SET video_url = 'https:' || video_url
            WHERE video_url LIKE '//%';
            """
        )

        # videoseries -> playlist?list=
        cur.execute(
            f"""
            UPDATE {storage_video}
            SET video_url = 'https://www.youtube.com/playlist?list=' ||
                regexp_replace(video_url, '.*[\\?&]list=([^&]+).*', '\\1')
            WHERE video_url ~* 'youtube(\\-nocookie)?\\.com/embed/videoseries'
              AND video_url ~* 'list=';
            """
        )

        # /embed/<id> -> /watch?v=<id>
        cur.execute(
            f"""
            UPDATE {storage_video}
            SET video_url = 'https://www.youtube.com/watch?v=' ||
                regexp_replace(video_url, '.*/embed/([^\\?&#/]+).*', '\\1')
            WHERE video_url ~* 'youtube(\\-nocookie)?\\.com/embed/'
              AND video_url !~* '/embed/videoseries';
            """
        )

        conn.commit()
        print(f"Đã migrate URL YouTube embed -> watch/playlist trong schema {schema}.")
    finally:
        if cur:
            cur.close()
        conn.close()


# Server DB
DB_HOST = POSTGRES_CONFIG.host
DB_PORT = POSTGRES_CONFIG.port
DB_NAME = POSTGRES_CONFIG.database
DB_USER = POSTGRES_CONFIG.username
DB_PASSWORD = POSTGRES_CONFIG.password
DB_SCHEMA = POSTGRES_CONFIG.schema


# Connection string (để dùng với psycopg2 hoặc SQLAlchemy)
def get_connection_string():
    return POSTGRES_CONFIG.dsn


def get_connection():
    """Trả về connection psycopg2 (nhớ đóng sau khi dùng)."""
    try:
        import psycopg2
    except ImportError:
        raise ImportError("Cần cài: pip install psycopg2-binary")
    cfg = load_postgres_config()
    conn = psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.database,
        user=cfg.username,
        password=cfg.password,
    )
    # Ensure unqualified table names resolve to desired schema
    try:
        with conn.cursor() as cur:
            cur.execute(
                'SET search_path TO "{}";'.format(str(cfg.schema).replace('"', '""'))
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return conn


def create_tables_if_not_exist(*, confirm: bool = False):
    """Tạo 3 bảng storage_data, storage_video, storage_image nếu chưa có."""
    _require_confirm(confirm, "create tables")
    conn = get_connection()
    cur = None
    try:
        cur = conn.cursor()
        schema = getattr(POSTGRES_CONFIG, "schema", None) or "public"
        cur.execute(build_create_tables_sql(schema))
        conn.commit()
        print(f"Đã tạo bảng (hoặc đã tồn tại) trong schema {schema}.")
    finally:
        if cur:
            cur.close()
        conn.close()


if __name__ == "__main__":
    import sys

    args = set(sys.argv[1:])
    confirm = "--confirm" in args

    if "create" in args:
        create_tables_if_not_exist(confirm=confirm)
    elif "migrate" in args:
        create_tables_if_not_exist(confirm=confirm)
        migrate_url_columns_to_text(confirm=confirm)
    elif "migrate_youtube" in args:
        create_tables_if_not_exist(confirm=confirm)
        migrate_url_columns_to_text(confirm=confirm)
        migrate_youtube_embed_urls_to_watch(confirm=confirm)
    else:
        conn = get_connection()
        print("Kết nối DB thành công.")
        conn.close()
