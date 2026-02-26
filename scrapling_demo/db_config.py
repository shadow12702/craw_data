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
    link VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS storage_video (
    id UUID PRIMARY KEY,
    storage_id UUID REFERENCES storage_data(id) ON DELETE CASCADE,
    video_url VARCHAR(500) NOT NULL
);

CREATE TABLE IF NOT EXISTS storage_image (
    id UUID PRIMARY KEY,
    storage_id UUID REFERENCES storage_data(id) ON DELETE CASCADE,
    image_url VARCHAR(500) NOT NULL
);
"""

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
    else:
        conn = get_connection()
        print("Kết nối DB thành công.")
        conn.close()
