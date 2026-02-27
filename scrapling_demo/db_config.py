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
    host="192.168.1.66",
    port=5432,
    database="postgres",
    username="n8n",
    password="n8n",
    sslmode="prefer",
)


def load_postgres_config() -> PostgresConfig:
    """Static config only (no .env, no env overrides)."""
    return POSTGRES_CONFIG


# -*- coding: utf-8 -*-
"""
Cấu hình kết nối PostgreSQL.
Server: http://192.168.1.66:5050 (giao diện web).
Kết nối trực tiếp DB dùng host/port bên dưới (PostgreSQL thường dùng port 5432).
"""

# Schema (chạy 1 lần để tạo bảng nếu chưa có):
CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS storage_data (
    id UUID PRIMARY KEY,
    title VARCHAR(255),
    content TEXT,
    link TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS storage_video (
    id UUID PRIMARY KEY,
    storage_id UUID REFERENCES storage_data(id) ON DELETE CASCADE,
    video_url TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS storage_image (
    id UUID PRIMARY KEY,
    storage_id UUID REFERENCES storage_data(id) ON DELETE CASCADE,
    image_url TEXT NOT NULL
);
"""


def migrate_url_columns_to_text() -> None:
    """
    Nâng giới hạn URL để tránh bị cắt (URL dài có token sẽ hỏng nếu bị truncate).
    An toàn khi chạy nhiều lần.
    """
    conn = get_connection()
    cur = None
    try:
        cur = conn.cursor()
        cur.execute("ALTER TABLE storage_data ALTER COLUMN link TYPE TEXT;")
        cur.execute("ALTER TABLE storage_video ALTER COLUMN video_url TYPE TEXT;")
        cur.execute("ALTER TABLE storage_image ALTER COLUMN image_url TYPE TEXT;")
        conn.commit()
        print("Đã migrate link/video_url/image_url sang TEXT.")
    finally:
        if cur:
            cur.close()
        conn.close()


def migrate_youtube_embed_urls_to_watch() -> None:
    """
    Chuyển URL YouTube dạng /embed/... trong storage_video sang dạng watch/playlist,
    đồng thời decode '&amp;' -> '&' và '//' -> 'https://'.
    """
    conn = get_connection()
    cur = None
    try:
        cur = conn.cursor()

        # Decode HTML entity first
        cur.execute(
            """
            UPDATE storage_video
            SET video_url = REPLACE(video_url, '&amp;', '&')
            WHERE video_url LIKE '%&amp;%';
            """
        )

        # Scheme-relative -> https
        cur.execute(
            """
            UPDATE storage_video
            SET video_url = 'https:' || video_url
            WHERE video_url LIKE '//%';
            """
        )

        # videoseries -> playlist?list=
        cur.execute(
            """
            UPDATE storage_video
            SET video_url = 'https://www.youtube.com/playlist?list=' ||
                regexp_replace(video_url, '.*[\\?&]list=([^&]+).*', '\\1')
            WHERE video_url ~* 'youtube(\\-nocookie)?\\.com/embed/videoseries'
              AND video_url ~* 'list=';
            """
        )

        # /embed/<id> -> /watch?v=<id>
        cur.execute(
            """
            UPDATE storage_video
            SET video_url = 'https://www.youtube.com/watch?v=' ||
                regexp_replace(video_url, '.*/embed/([^\\?&#/]+).*', '\\1')
            WHERE video_url ~* 'youtube(\\-nocookie)?\\.com/embed/'
              AND video_url !~* '/embed/videoseries';
            """
        )

        conn.commit()
        print("Đã migrate URL YouTube embed -> watch/playlist trong storage_video.")
    finally:
        if cur:
            cur.close()
        conn.close()

# Server DB
DB_HOST = "192.168.1.66"
DB_PORT = 5432  # PostgreSQL mặc định; nếu DB chạy ở 5050 thì đổi thành 5050
DB_NAME = "postgres"  # tên database, đổi nếu dùng DB khác
DB_USER = "n8n"
DB_PASSWORD = "n8n"

# Connection string (để dùng với psycopg2 hoặc SQLAlchemy)
def get_connection_string():
    return (
        f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )


def get_connection():
    """Trả về connection psycopg2 (nhớ đóng sau khi dùng)."""
    try:
        import psycopg2
    except ImportError:
        raise ImportError("Cần cài: pip install psycopg2-binary")
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def create_tables_if_not_exist():
    """Tạo 3 bảng storage_data, storage_video, storage_image nếu chưa có."""
    conn = get_connection()
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(CREATE_TABLES_SQL)
        conn.commit()
        print("Đã tạo bảng (hoặc đã tồn tại).")
    finally:
        if cur:
            cur.close()
        conn.close()


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "create":
        create_tables_if_not_exist()
    elif len(sys.argv) > 1 and sys.argv[1] == "migrate":
        create_tables_if_not_exist()
        migrate_url_columns_to_text()
    elif len(sys.argv) > 1 and sys.argv[1] == "migrate_youtube":
        create_tables_if_not_exist()
        migrate_url_columns_to_text()
        migrate_youtube_embed_urls_to_watch()
    else:
        conn = get_connection()
        print("Kết nối DB thành công.")
        conn.close()
