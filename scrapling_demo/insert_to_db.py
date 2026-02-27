# -*- coding: utf-8 -*-
"""
Đọc all_data.json, kết nối DB (db_config) và INSERT vào storage_data, storage_video, storage_image.
Chạy: python insert_to_db.py [đường_dẫn_all_data.json]
Mặc định: csv_by_nav/all_data.json (so với thư mục gốc repo).
"""
import json
import os
import sys
import uuid
from html import unescape
from urllib.parse import parse_qs, urlencode, urlparse
import re

# Thư mục gốc repo (parent của scrapling_demo)
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_JSON = os.path.join(REPO_ROOT, "csv_by_nav", "all_data.json")


def canonicalize_video_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    if u.startswith("//"):
        u = "https:" + u
    try:
        p = urlparse(u)
    except Exception:
        return u

    host = (p.netloc or "").lower()
    path = p.path or ""

    def _watch_url(video_id: str, query: str) -> str:
        q = parse_qs(query or "", keep_blank_values=False)
        for k in ("feature", "si"):
            q.pop(k, None)
        params: list[tuple[str, str]] = [("v", video_id)]
        for k, vals in q.items():
            for v in vals:
                if v is not None and v != "":
                    params.append((k, v))
        return "https://www.youtube.com/watch?" + urlencode(params, doseq=True)

    if host == "youtu.be":
        vid = path.strip("/").split("/")[0] if path else ""
        return _watch_url(vid, p.query) if vid else u

    if host.endswith("youtube.com") or host.endswith("youtube-nocookie.com"):
        if path.startswith("/embed/videoseries"):
            q = parse_qs(p.query or "")
            lst = (q.get("list") or [None])[0]
            if lst:
                return f"https://www.youtube.com/playlist?list={lst}"
            return u
        m = re.match(r"^/embed/([^/?#]+)", path or "")
        if m:
            return _watch_url(m.group(1), p.query)

    return u


def insert_from_json(json_path: str, create_tables: bool = True) -> None:
    from db_config import get_connection, create_tables_if_not_exist, migrate_url_columns_to_text

    if create_tables:
        create_tables_if_not_exist()
        migrate_url_columns_to_text()

    if not os.path.isfile(json_path):
        print(f"Không tìm thấy file: {json_path}")
        sys.exit(1)

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not data:
        print("all_data.json rỗng.")
        return

    conn = get_connection()
    try:
        cur = conn.cursor()
        inserted_data = 0
        inserted_video = 0
        inserted_image = 0

        for row in data:
            sid = row.get("id")
            title = (row.get("title") or "")[:255]
            content = row.get("content") or ""
            link = unescape((row.get("link") or "").strip())
            created_at = row.get("created_at") or None

            # created_at: nếu None/rỗng thì dùng CURRENT_TIMESTAMP
            cur.execute(
                """
                INSERT INTO storage_data (id, title, content, link, created_at)
                VALUES (%s::uuid, %s, %s, %s, COALESCE(%s::timestamp, CURRENT_TIMESTAMP))
                """,
                (sid, title, content, link, created_at or None),
            )
            inserted_data += 1

            for v in row.get("video_links") or []:
                if v and isinstance(v, str) and v.strip():
                    vv = canonicalize_video_url(unescape(v.strip()))
                    cur.execute(
                        """
                        INSERT INTO storage_video (id, storage_id, video_url)
                        VALUES (%s::uuid, %s::uuid, %s)
                        """,
                        (str(uuid.uuid4()), sid, vv),
                    )
                    inserted_video += 1

            for img in row.get("image_urls") or []:
                if img and isinstance(img, str) and img.strip():
                    cur.execute(
                        """
                        INSERT INTO storage_image (id, storage_id, image_url)
                        VALUES (%s::uuid, %s::uuid, %s)
                        """,
                        (str(uuid.uuid4()), sid, unescape(img.strip())),
                    )
                    inserted_image += 1

        conn.commit()
        print(f"Đã insert: storage_data={inserted_data}, storage_video={inserted_video}, storage_image={inserted_image}")
    except Exception as e:
        conn.rollback()
        print(f"Lỗi: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_JSON
    print(f"Đọc: {path}")
    insert_from_json(path)
