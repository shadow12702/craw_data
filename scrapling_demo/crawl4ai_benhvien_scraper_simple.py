import asyncio
import csv
import hashlib
import json
import logging
import re
import os
import uuid
import pandas as pd
import importlib.util
from html.parser import HTMLParser
from html import unescape
from datetime import datetime
from urllib.parse import parse_qs, urlencode, urljoin, urlparse
from crawl4ai import AsyncWebCrawler
import aiohttp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now().isoformat()


def canonicalize_video_url(url: str) -> str:
    """
    Chuẩn hóa URL video để dễ click từ DB.
    Đặc biệt: chuyển YouTube embed (/embed/..., /embed/videoseries) -> watch/playlist.
    """
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


class CrawlState:
    """
    Append-only event log để hỗ trợ resume khi đang chạy bị stop.

    Lưu tại: <output_root>/state/<events_filename>
    """

    def __init__(self, output_root: str, events_filename: str = "events_db.jsonl"):
        self.state_dir = os.path.join(output_root or ".", "state")
        os.makedirs(self.state_dir, exist_ok=True)
        self.events_path = os.path.join(self.state_dir, events_filename)
        self._lock = asyncio.Lock()

    async def record(self, event_type: str, url: str, ok: bool | None = None, error: str | None = None) -> None:
        rec: dict[str, object] = {"ts": _now_iso(), "type": event_type, "url": url}
        if ok is not None:
            rec["ok"] = bool(ok)
        if error:
            rec["error"] = str(error)[:500]
        line = json.dumps(rec, ensure_ascii=False)
        async with self._lock:
            with open(self.events_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")

    def load(self) -> tuple[set[str], set[str], set[str], set[str]]:
        """
        Returns (enqueued, started, done_ok, done_failed)
        """
        enqueued: set[str] = set()
        started: set[str] = set()
        done_ok: set[str] = set()
        done_failed: set[str] = set()

        if not os.path.exists(self.events_path):
            return enqueued, started, done_ok, done_failed

        try:
            with open(self.events_path, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except Exception:
                        continue
                    t = rec.get("type")
                    u = rec.get("url")
                    if not isinstance(u, str) or not u:
                        continue
                    if t == "enqueue":
                        enqueued.add(u)
                    elif t == "start":
                        started.add(u)
                    elif t == "done":
                        if rec.get("ok") is True:
                            done_ok.add(u)
                        else:
                            done_failed.add(u)
        except Exception:
            return enqueued, started, done_ok, done_failed

        return enqueued, started, done_ok, done_failed


class _ContentTextExtractor(HTMLParser):
    """
    Trích text từ HTML, bỏ phần nằm trong element có class 'header-wrapper'
    và bỏ cả các vùng menu/nav (header/nav/footer/aside) để tránh menu bị lẫn vào content.
    Không dùng bs4 để tránh thêm dependency.
    """

    _BLOCK_TAGS = {
        "p", "br", "div", "section", "article", "main",
        "h1", "h2", "h3", "h4", "h5", "h6",
        "ul", "ol", "li",
        "table", "thead", "tbody", "tr", "td", "th",
        "blockquote",
    }

    def __init__(self, exclude_class_contains: set[str] | None = None, *, skip_nav_menu: bool = True):
        super().__init__(convert_charrefs=False)
        self.exclude_class_contains = exclude_class_contains or {"header-wrapper"}
        self.skip_nav_menu = bool(skip_nav_menu)
        self._exclude_depth = 0
        self._skip_depth = 0  # script/style/noscript + nav/menu areas
        self._chunks: list[str] = []

    def _attrs_dict(self, attrs):
        try:
            return {k: v for k, v in (attrs or [])}
        except Exception:
            return {}

    def _is_excluded_start(self, tag: str, attrs) -> bool:
        if tag.lower() != "div":
            return False
        ad = self._attrs_dict(attrs)
        cls = ad.get("class") or ""
        if isinstance(cls, (list, tuple)):
            cls = " ".join(str(x) for x in cls if x)
        cls = str(cls)
        return any(x in cls for x in self.exclude_class_contains)

    def _is_nav_menu_start(self, tag: str, attrs) -> bool:
        t = (tag or "").lower()
        if t in ("nav", "header", "footer", "aside"):
            return True
        ad = self._attrs_dict(attrs)
        cls = ad.get("class") or ""
        if isinstance(cls, (list, tuple)):
            cls = " ".join(str(x) for x in cls if x)
        cls = str(cls).lower()
        # heuristic: các wrapper menu phổ biến
        if any(k in cls for k in ("menu", "navbar", "nav", "site-header", "site-footer", "header", "footer")):
            return True
        return False

    def handle_starttag(self, tag, attrs):
        tag = (tag or "").lower()
        if self._exclude_depth > 0:
            self._exclude_depth += 1
            return
        if self._skip_depth > 0:
            self._skip_depth += 1
            return
        if tag in ("script", "style", "noscript"):
            self._skip_depth = 1
            return
        if self.skip_nav_menu and self._is_nav_menu_start(tag, attrs):
            self._skip_depth = 1
            return
        if self._is_excluded_start(tag, attrs):
            self._exclude_depth = 1
            return
        if tag in self._BLOCK_TAGS:
            self._chunks.append("\n")

    def handle_endtag(self, tag):
        if self._exclude_depth > 0:
            self._exclude_depth -= 1
            return
        if self._skip_depth > 0:
            self._skip_depth -= 1
            return
        tag = (tag or "").lower()
        if tag in self._BLOCK_TAGS:
            self._chunks.append("\n")

    def handle_data(self, data):
        if self._exclude_depth > 0 or self._skip_depth > 0:
            return
        if data:
            self._chunks.append(data)

    def handle_entityref(self, name):
        if self._exclude_depth > 0 or self._skip_depth > 0:
            return
        self._chunks.append(f"&{name};")

    def handle_charref(self, name):
        if self._exclude_depth > 0 or self._skip_depth > 0:
            return
        self._chunks.append(f"&#{name};")

    def text(self) -> str:
        return unescape("".join(self._chunks))


class _ElementInnerHTMLExtractor(HTMLParser):
    """Lấy inner HTML của 1 element mục tiêu (theo tag/id/class)."""

    def __init__(
        self,
        *,
        tag: str,
        required_classes: set[str] | None = None,
        id_equals: str | None = None,
    ):
        super().__init__(convert_charrefs=False)
        self.tag = (tag or "").lower()
        self.required_classes = required_classes or set()
        self.id_equals = id_equals
        self._depth = 0
        self._capturing = False
        self._chunks: list[str] = []

    @staticmethod
    def _attrs_dict(attrs) -> dict[str, str]:
        try:
            return {k.lower(): (v or "") for k, v in (attrs or []) if k}
        except Exception:
            return {}

    @staticmethod
    def _class_has_all(cls: str, required: set[str]) -> bool:
        parts = {p for p in re.split(r"\s+", (cls or "").strip()) if p}
        return required.issubset(parts)

    def _is_target(self, tag: str, attrs) -> bool:
        if (tag or "").lower() != self.tag:
            return False
        ad = self._attrs_dict(attrs)
        if self.id_equals:
            if str(ad.get("id") or "") != self.id_equals:
                return False
        cls = str(ad.get("class") or "")
        return self._class_has_all(cls, self.required_classes)

    @staticmethod
    def _rebuild_start(tag: str, attrs) -> str:
        if not attrs:
            return f"<{tag}>"
        parts = []
        for k, v in attrs:
            if not k:
                continue
            if v is None:
                parts.append(str(k))
            else:
                vv = str(v).replace('"', "&quot;")
                parts.append(f'{k}="{vv}"')
        return f"<{tag} {' '.join(parts)}>"

    def handle_starttag(self, tag, attrs):
        if not self._capturing:
            if self._is_target(tag, attrs):
                self._capturing = True
                self._depth = 1
            return
        # capturing inner content
        self._depth += 1
        self._chunks.append(self._rebuild_start(tag, attrs))

    def handle_startendtag(self, tag, attrs):
        if not self._capturing:
            return
        self._chunks.append(self._rebuild_start(tag, attrs)[:-1] + " />")

    def handle_endtag(self, tag):
        if not self._capturing:
            return
        self._depth -= 1
        if self._depth <= 0:
            self._capturing = False
            self._depth = 0
            return
        self._chunks.append(f"</{tag}>")

    def handle_data(self, data):
        if self._capturing and data:
            self._chunks.append(data)

    def handle_entityref(self, name):
        if self._capturing:
            self._chunks.append(f"&{name};")

    def handle_charref(self, name):
        if self._capturing:
            self._chunks.append(f"&#{name};")

    def html(self) -> str:
        return "".join(self._chunks)


class PostgresWriterSimple:
    def __init__(self):
        self._db_mod = None
        self._tables_ready = False

    def _load_db_config(self):
        if self._db_mod is not None:
            return self._db_mod
        here = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(here, "db_config.py")
        spec = importlib.util.spec_from_file_location("_scraper_db_config", path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Cannot load db_config.py from: {path}")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        self._db_mod = mod
        return mod

    def _ensure_tables(self) -> None:
        if self._tables_ready:
            return
        mod = self._load_db_config()
        conn = mod.get_connection()
        cur = None
        try:
            cur = conn.cursor()
            cur.execute(mod.CREATE_TABLES_SQL)
            conn.commit()
            self._tables_ready = True
        finally:
            if cur:
                cur.close()
            conn.close()

    def upsert_page(self, storage_id: uuid.UUID, title: str, content: str, created_at: datetime, link: str,
                    video_links: list[str] | None = None, image_urls: list[str] | None = None) -> None:
        self._ensure_tables()
        mod = self._load_db_config()
        conn = mod.get_connection()
        cur = None
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO storage_data (id, title, content, created_at, link)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE
                SET title = EXCLUDED.title,
                    content = EXCLUDED.content,
                    created_at = EXCLUDED.created_at,
                    link = EXCLUDED.link;
                """,
                (str(storage_id), (title or "")[:255], content or "", created_at, (link or "")),
            )

            for v in (video_links or []):
                v = unescape((v or "").strip())
                v = canonicalize_video_url(v)
                if not v:
                    continue
                vid = uuid.uuid5(storage_id, f"video:{v}")
                cur.execute(
                    """
                    INSERT INTO storage_video (id, storage_id, video_url)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (id) DO NOTHING;
                    """,
                    (str(vid), str(storage_id), v),
                )

            for u in (image_urls or []):
                u = unescape((u or "").strip())
                if not u:
                    continue
                iid = uuid.uuid5(storage_id, f"image:{u}")
                cur.execute(
                    """
                    INSERT INTO storage_image (id, storage_id, image_url)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (id) DO NOTHING;
                    """,
                    (str(iid), str(storage_id), u),
                )

            conn.commit()
        finally:
            if cur:
                cur.close()
            conn.close()


class CrawlAIBenhVienScraperSimple:
    """
    Luồng: Đọc các link trong file CSV (benhvien_data.csv) → truy cập từng link
    → crawl dữ liệu thật từ trang (title, content, link/video) → xuất ra JSON + CSV.

    - Input: CSV có cột link (hoặc url).
    - Output: JSON và CSV chứa dữ liệu đã crawl (title, url, content, link, navigation).
    Trang home có navigation dạng tree node.
    """

    def __init__(
        self,
        input_csv: str | list[str] = "benhvien_data.csv",
        max_pages: int = None,
        concurrency: int = 10,
        resume: bool = True,
        retry_failed_on_resume: bool = False,
    ):
        # Cho phép 1 file hoặc nhiều file CSV
        self.input_csv = input_csv if isinstance(input_csv, list) else [input_csv]
        self.base_domain = "benhvienanbinh.vn"
        self.max_pages = max_pages
        self.concurrency = concurrency  # Số URL crawl song song
        self.resume = bool(resume)
        self.retry_failed_on_resume = bool(retry_failed_on_resume)
        self.crawler = None
        self.visited_urls = set()
        self.content_data = []  # {url, title, content, link (video+links), navigation}
        self.urls_to_process = []  # list of (url, title_from_csv or None)
        self.home_url = None
        self._semaphore = None
        self._csv_file = None
        self._csv_writer = None
        self._jsonl_file = None
        self._state: CrawlState | None = None
        self._db_writer: PostgresWriterSimple | None = PostgresWriterSimple()
        # Cây nav tăng dần khi ghi từng kết quả (để gán nav_id cho CSV/JSONL)
        self._nav_key_to_id: dict[tuple[str, int], int] = {}
        self._nav_next_id = 2
        # Khi đọc từ file markdown * [Label](URL): nhóm theo label để xuất CSV riêng
        self._label_to_urls: dict[str, list[str]] = {}

    def _set_state(self, output_root: str, events_filename: str = "events_db.jsonl") -> None:
        self._state = CrawlState(output_root=output_root or ".", events_filename=events_filename)

    def _canonical_url(self, url: str) -> str:
        return self.normalize_path((url or "").strip())

    def _storage_uuid_from_url(self, url: str) -> uuid.UUID:
        # UUID ổn định theo URL -> resume/upsert không tạo trùng
        return uuid.uuid5(uuid.NAMESPACE_URL, self._canonical_url(url))

    def _extract_text_excluding_header_wrapper(self, html: str) -> str:
        if not html:
            return ""
        p = _ContentTextExtractor(exclude_class_contains={"header-wrapper"})
        try:
            p.feed(html)
            p.close()
        except Exception:
            return ""
        return p.text()

    def _extract_text_full_page(self, html: str) -> str:
        """Fallback cuối: lấy toàn bộ nội dung trang (không skip nav/menu, không bỏ header-wrapper)."""
        if not html:
            return ""
        p = _ContentTextExtractor(exclude_class_contains=set(), skip_nav_menu=False)
        try:
            p.feed(html)
            p.close()
        except Exception:
            return ""
        return p.text()

    def _extract_main_content_html(self, html: str) -> str:
        if not html:
            return ""
        # Ưu tiên theo template WP của benhvienanbinh.vn
        candidates = [
            dict(tag="div", required_classes={"entry-content", "single-page"}, id_equals=None),
            dict(tag="div", required_classes={"entry-content"}, id_equals=None),
            dict(tag="article", required_classes=set(), id_equals=None),
            dict(tag="main", required_classes=set(), id_equals=None),
            dict(tag="div", required_classes=set(), id_equals="content"),
        ]
        for cfg in candidates:
            p = _ElementInnerHTMLExtractor(**cfg)
            try:
                p.feed(html)
                p.close()
                out = p.html()
                if out and len(out.strip()) > 200:
                    return out
            except Exception:
                continue
        return ""

    def _extract_page_content(self, markdown: str, html: str, url: str) -> str:
        # Ưu tiên vùng bài viết (entry-content / article / main ...)
        main_html = self._extract_main_content_html(html or "")
        if main_html:
            text = self._extract_text_excluding_header_wrapper(main_html)
        else:
            # fallback cuối: lấy toàn bộ nội dung trang
            text = self._extract_text_full_page(html or "")
        text = self._strip_urls_from_content(text)
        text = self._clean_content(text)
        if len(text) >= 80:
            return text
        # Fallback sang markdown nếu HTML text quá ngắn
        md = self._strip_urls_from_content(markdown or "")
        return self._clean_content(md)

    async def _push_to_db(self, result: dict) -> None:
        if not self._db_writer:
            return
        url = (result.get("url") or "").strip()
        if not url:
            return
        storage_id = self._storage_uuid_from_url(url)
        title = (result.get("title") or "").strip()
        content = result.get("content") or ""
        ts = result.get("timestamp") or ""
        created_at = None
        try:
            created_at = datetime.fromisoformat(ts) if ts else None
        except Exception:
            created_at = None
        if created_at is None:
            created_at = datetime.now()
        video_links = result.get("video_links") or []
        image_urls = result.get("image_urls") or []
        link = url
        await asyncio.to_thread(
            self._db_writer.upsert_page,
            storage_id,
            title,
            content,
            created_at,
            link,
            list(video_links) if isinstance(video_links, (list, tuple)) else [],
            list(image_urls) if isinstance(image_urls, (list, tuple)) else [],
        )

    def load_links_from_markdown(self, filepath: str) -> bool:
        """
        Đọc file chứa link dạng markdown: * [Label](URL) hoặc - [Label](URL).
        Gán self.urls_to_process = (url, label) cho từng URL (crawl mỗi URL một lần),
        self._label_to_urls = {label: [url1, url2, ...]} để sau này chia CSV theo label.
        """
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                text = f.read()
        except FileNotFoundError:
            logger.error(f"File not found: {filepath}")
            return False

        # * [Label](URL) hoặc - [Label](URL)
        pattern = re.compile(r"^\s*[-*]\s*\[([^\]]+)\]\((\S+)\)", re.MULTILINE)
        pairs = pattern.findall(text)
        if not pairs:
            logger.warning(f"No * [Label](URL) lines found in {filepath}")
            return False

        self._label_to_urls = {}
        seen_urls = set()
        self.urls_to_process = []

        for label, url in pairs:
            label = label.strip()
            url = url.strip()
            if not url.startswith("http"):
                continue
            if label not in self._label_to_urls:
                self._label_to_urls[label] = []
            self._label_to_urls[label].append(url)
            if url not in seen_urls:
                seen_urls.add(url)
                self.urls_to_process.append((url, label))

        if self.urls_to_process:
            self.home_url = self.normalize_path(self.urls_to_process[0][0])
        logger.info(
            f"Loaded {len(self.urls_to_process)} unique URLs, {len(self._label_to_urls)} labels from {filepath}"
        )
        return True

    def load_urls_from_csv(self):
        """Load URLs từ một hoặc nhiều file CSV (cột 'link'/'url', 'title' nếu có)."""
        try:
            self.urls_to_process = []
            seen_urls = set()

            for csv_path in self.input_csv:
                df = pd.read_csv(csv_path)
                link_col = "link" if "link" in df.columns else "url"
                title_col = "title" if "title" in df.columns else None

                urls = df[link_col].dropna().astype(str).unique().tolist()
                for u in urls:
                    u = u.strip()
                    if not u or not u.startswith("http") or u in seen_urls:
                        continue
                    seen_urls.add(u)
                    row = df[df[link_col].astype(str).str.strip() == u].iloc[0]
                    if title_col and pd.notna(row.get(title_col)):
                        title_from_csv = str(row[title_col]).strip() or None
                    else:
                        title_from_csv = None
                    self.urls_to_process.append((u, title_from_csv))

                logger.info(
                    f"Loaded {len(urls)} URLs from {csv_path} (column: {link_col})"
                )

            if not self.urls_to_process:
                logger.warning("No URLs loaded from any CSV")
                return False

            if self.max_pages:
                self.urls_to_process = self.urls_to_process[: self.max_pages]
                logger.info(f"Limited to {self.max_pages} pages")

            logger.info(
                f"Total {len(self.urls_to_process)} unique URLs from {len(self.input_csv)} file(s)"
            )
            if self.urls_to_process:
                self.home_url = self.normalize_path(self.urls_to_process[0][0])
        except FileNotFoundError as e:
            logger.error(f"CSV file not found: {e}")
            return False
        return True

    def normalize_path(self, url: str) -> str:
        """Chuẩn hóa URL để so sánh (bỏ fragment, trailing slash)"""
        p = urlparse(url)
        path = (p.path or "/").rstrip("/") or "/"
        return f"{p.scheme}://{p.netloc}{path}"

    def _domain_from_url(self, url: str) -> str:
        """Lấy domain từ URL (để crawl cùng site)."""
        try:
            return urlparse(url).netloc or ""
        except Exception:
            return ""

    def extract_internal_links(self, html: str, base_url: str) -> set[str]:
        """Chỉ lấy link nội bộ (cùng domain) từ <a href> để mở rộng crawl (trang con)."""
        out = set()
        if not html:
            return out
        domain = self._domain_from_url(base_url) or self.base_domain
        try:
            for m in re.finditer(r'<a[^>]+href=[\'"]([^\'"]+)[\'"]', html, re.IGNORECASE):
                href = m.group(1).strip()
                if href.startswith("#") or href.startswith("javascript:") or href.startswith("mailto:"):
                    continue
                full = urljoin(base_url, href)
                p = urlparse(full)
                if domain and domain in (p.netloc or ""):
                    out.add(full)
        except Exception as e:
            logger.debug(f"extract_internal_links: {e}")
        return out

    def _assign_nav_id_incremental(self, result: dict) -> None:
        """Cập nhật cây nav tăng dần và gán result['nav_id'] (node lá)."""
        if not hasattr(self, "_nav_key_to_id") or not self._nav_key_to_id:
            self._nav_key_to_id = {("Home", 0): 1}
            self._nav_next_id = 2
        parent_id = 1
        path = self._parse_nav_to_path(result.get("navigation") or "Home")
        for level, label in path:
            if not label:
                continue
            key = (label, parent_id)
            if key in self._nav_key_to_id:
                nid = self._nav_key_to_id[key]
            else:
                nid = self._nav_next_id
                self._nav_next_id += 1
                self._nav_key_to_id[key] = nid
            parent_id = nid
        result["nav_id"] = parent_id

    def _write_result_immediately(self, result: dict):
        """Ghi ngay 1 kết quả vào CSV và JSONL (có nav_id, không đợi crawl hết)."""
        self._assign_nav_id_incremental(result)
        self.content_data.append(result)
        video_col = self._links_to_column(result.get("video_links", []))
        image_urls_col = self._links_to_column(result.get("image_urls", []))
        if self._csv_writer:
            self._csv_writer.writerow([
                self._clean_text(result.get("title", "")),
                (result.get("url", "") or "").strip(),
                result.get("nav_id", ""),
                self._clean_content(result.get("content", "")),
                video_col,
                image_urls_col,
                result.get("timestamp", ""),
            ])
            self._csv_file.flush()
        if self._jsonl_file:
            self._jsonl_file.write(json.dumps(result, ensure_ascii=False) + "\n")
            self._jsonl_file.flush()

    async def _fetch_one(self, url: str, title_from_csv: str = None) -> dict | None:
        """Một task crawl 1 URL (dùng semaphore để giới hạn đồng thời)."""
        canon = self._canonical_url(url)
        if canon in self.visited_urls:
            return None
        self.visited_urls.add(canon)

        async with self._semaphore:
            logger.info(
                f"Fetching: {url} ({len(self.visited_urls)}/{len(self.urls_to_process)})"
            )
            err = None
            if self._state and self.resume:
                await self._state.record("start", canon)
            try:
                result = await self._do_fetch(url, title_from_csv)
                if result:
                    self._write_result_immediately(result)
                    await self._push_to_db(result)
                    if self._state and self.resume:
                        await self._state.record("done", canon, ok=True)
                else:
                    err = "fetch_failed"
                    if self._state and self.resume:
                        await self._state.record("done", canon, ok=False, error=err)
                return result
            except Exception as e:
                err = str(e)
                logger.error(f"Fetch error {url}: {err}")
                if self._state and self.resume:
                    await self._state.record("done", canon, ok=False, error=err)
                return None

    async def _do_fetch(self, url: str, title_from_csv: str = None) -> dict | None:
        """Thực hiện crawl 1 URL (gọi từ _fetch_one)."""
        try:
            result = await self.crawler.arun(
                url=url, bypass_cache=True, wait_until="network_idle"
            )

            if result.success:
                html = result.html or ""
                main_html = self._extract_main_content_html(html)
                html_for_media = main_html or html
                # Title: ưu tiên từ CSV, không có thì lấy từ page
                title = title_from_csv
                if not title:
                    title = self.extract_title(result.markdown or "")
                if not title:
                    title = self.extract_title_from_html(html)

                # Content: bỏ phần header-wrapper để tránh trùng content, đồng thời bỏ URL
                raw_content = self._extract_page_content(result.markdown or "", html or "", url)

                # Tất cả link + video trong page
                page_links = self.extract_links_and_videos(html_for_media, url)
                video_links = self.extract_video_links_only(html_for_media, url)
                internal_links = self.extract_internal_links(html, url)

                # Navigation: trang home thì dạng tree node
                is_home = self.normalize_path(url) == self.home_url
                navigation = self.extract_navigation_tree(html, url) if is_home else self.extract_navigation(html)

                # Ảnh: trích URL
                image_urls = self.extract_image_urls(html_for_media, url)
                images_downloaded = []
                page_folder = ""

                # Mỗi page một folder riêng: data + ảnh trong folder đó
                pages_dir = getattr(self, "_pages_dir", None)
                if pages_dir:
                    page_slug = self._page_slug(url, title)
                    page_folder = os.path.join(pages_dir, page_slug)
                    os.makedirs(page_folder, exist_ok=True)
                    if getattr(self, "_download_images", False) and image_urls:
                        images_dir = os.path.join(page_folder, "images")
                        images_downloaded = await self._download_images_batch(
                            image_urls, images_dir, concurrency=5
                        )
                        rel_paths = [os.path.relpath(p, page_folder) for p in images_downloaded]
                    else:
                        rel_paths = []
                    data = {
                        "title": title,
                        "url": url,
                        "content": raw_content,
                        "video_links": video_links,
                        "image_urls": image_urls,
                        "images_downloaded": rel_paths,
                        "timestamp": datetime.now().isoformat(),
                    }
                    with open(os.path.join(page_folder, "data.json"), "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                    with open(os.path.join(page_folder, "content.md"), "w", encoding="utf-8") as f:
                        f.write(f"# {title}\n\n**URL:** {url}\n\n---\n\n{raw_content}")
                elif getattr(self, "_download_images", False) and getattr(self, "_images_dir", None):
                    images_downloaded = await self._download_images_batch(
                        image_urls, self._images_dir, concurrency=5
                    )

                return {
                    "url": url,
                    "title": title,
                    "content": raw_content,
                    "link": page_links,
                    "video_links": video_links,
                    "internal_links": list(internal_links),
                    "image_urls": image_urls,
                    "images_downloaded": images_downloaded,
                    "page_folder": page_folder,
                    "navigation": navigation,
                    "timestamp": datetime.now().isoformat(),
                }
            else:
                logger.warning(f"Failed to fetch {url}")
                return None

        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None

    def extract_links_and_videos(self, html: str, base_url: str) -> list:
        """Trích tất cả link và video (src/embed) từ HTML vào cột link"""
        if not html:
            return []
        links = set()
        try:
            parsed = urlparse(base_url)
            base = f"{parsed.scheme}://{parsed.netloc}"

            # <a href="...">
            for m in re.finditer(r'<a[^>]+href=[\'"]([^\'"]+)[\'"]', html, re.IGNORECASE):
                href = m.group(1).strip()
                if href.startswith("#") or href.startswith("javascript:"):
                    continue
                full = urljoin(base_url, href)
                if self.base_domain in urlparse(full).netloc or full.startswith("http"):
                    links.add(full)

            # <video src="...">, <source src="...">
            for tag in ("video", "source", "audio"):
                for m in re.finditer(
                    rf'<{tag}[^>]+src=[\'"]([^\'"]+)[\'"]',
                    html, re.IGNORECASE
                ):
                    links.add(urljoin(base_url, m.group(1).strip()))

            # <iframe src="..."> (YouTube, video embed)
            for m in re.finditer(r'<iframe[^>]+src=[\'"]([^\'"]+)[\'"]', html, re.IGNORECASE):
                links.add(urljoin(base_url, m.group(1).strip()))

            # data-src (lazy load)
            for m in re.finditer(r'data-src=[\'"]([^\'"]+)[\'"]', html, re.IGNORECASE):
                links.add(urljoin(base_url, m.group(1).strip()))
        except Exception as e:
            logger.debug(f"extract_links_and_videos: {e}")
        return list(links)

    def extract_video_links_only(self, html: str, base_url: str) -> list[str]:
        """Chỉ trích link video/audio/embed (iframe, video, source, audio) để quản lý riêng."""
        out = []
        seen = set()
        if not html:
            return out
        try:
            for tag in ("video", "source", "audio"):
                for m in re.finditer(
                    rf'<{tag}[^>]+src=[\'"]([^\'"]+)[\'"]',
                    html, re.IGNORECASE
                ):
                    u = urljoin(base_url, unescape(m.group(1).strip()))
                    u = canonicalize_video_url(u)
                    # Tránh nhầm ảnh vào video (một số site nhét ảnh qua src)
                    if self._is_image_url(u):
                        continue
                    if u not in seen:
                        seen.add(u)
                        out.append(u)

            # Iframe embed: ưu tiên src, fallback data-src (nhưng chỉ trong tag iframe)
            for m in re.finditer(r"<iframe\b[^>]*>", html, flags=re.IGNORECASE):
                tag_html = m.group(0) or ""
                src_m = re.search(r'\bsrc=[\'"]([^\'"]+)[\'"]', tag_html, flags=re.IGNORECASE)
                if not src_m:
                    src_m = re.search(r'\bdata-src=[\'"]([^\'"]+)[\'"]', tag_html, flags=re.IGNORECASE)
                if not src_m:
                    continue
                u = urljoin(base_url, unescape((src_m.group(1) or "").strip()))
                u = canonicalize_video_url(u)
                if self._is_image_url(u):
                    continue
                if u not in seen:
                    seen.add(u)
                    out.append(u)
        except Exception as e:
            logger.debug(f"extract_video_links_only: {e}")
        return out

    def extract_image_urls(self, html: str, base_url: str) -> list[str]:
        """Trích tất mọi URL ảnh từ HTML (img src, data-src, srcset)."""
        out = []
        seen = set()
        if not html:
            return out
        try:
            # <img src="...">
            for m in re.finditer(r'<img[^>]+src=[\'"]([^\'"]+)[\'"]', html, re.IGNORECASE):
                u = urljoin(base_url, unescape(m.group(1).strip()))
                if u not in seen and self._is_image_url(u):
                    seen.add(u)
                    out.append(u)
            # data-src, data-lazy-src (lazy load)
            for m in re.finditer(r'data-(?:lazy-)?src=[\'"]([^\'"]+)[\'"]', html, re.IGNORECASE):
                u = urljoin(base_url, unescape(m.group(1).strip()))
                if u not in seen and self._is_image_url(u):
                    seen.add(u)
                    out.append(u)
            # srcset="url 1x, url2 2x"
            for m in re.finditer(r'srcset=[\'"]([^\'"]+)[\'"]', html, re.IGNORECASE):
                for part in m.group(1).split(","):
                    part = part.strip().split()[0] if part.strip() else ""
                    if part:
                        u = urljoin(base_url, unescape(part.strip()))
                        if u not in seen and self._is_image_url(u):
                            seen.add(u)
                            out.append(u)
        except Exception as e:
            logger.debug(f"extract_image_urls: {e}")
        return out

    def _is_image_url(self, url: str) -> bool:
        """Kiểm tra URL có phải ảnh (theo extension hoặc pattern)."""
        u = url.lower().split("?")[0]
        return any(u.endswith(e) for e in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".bmp", ".ico"))

    async def _download_images_batch(
        self, image_urls: list[str], save_dir: str, concurrency: int = 5
    ) -> list[str]:
        """Tải từng ảnh về save_dir, trả về list đường dẫn file đã lưu (relative hoặc absolute)."""
        if not image_urls or not save_dir:
            return []
        os.makedirs(save_dir, exist_ok=True)
        semaphore = asyncio.Semaphore(concurrency)
        saved = []

        async def download_one(img_url: str) -> str | None:
            async with semaphore:
                try:
                    h = hashlib.sha256(img_url.encode()).hexdigest()[:16]
                    ext = ".jpg"
                    for e in (".png", ".gif", ".webp", ".jpeg", ".jpg", ".svg"):
                        if e in img_url.lower().split("?")[0]:
                            ext = e if e != ".jpeg" else ".jpg"
                            break
                    path = os.path.join(save_dir, f"{h}{ext}")
                    if os.path.isfile(path):
                        saved.append(path)
                        return path
                    async with aiohttp.ClientSession() as session:
                        async with session.get(img_url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                            if resp.status != 200:
                                return None
                            content = await resp.read()
                    if len(content) < 100:
                        return None
                    with open(path, "wb") as f:
                        f.write(content)
                    return path
                except Exception as e:
                    logger.debug(f"Download image {img_url[:60]}: {e}")
                    return None

        tasks = [download_one(u) for u in image_urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, str):
                saved.append(r)
            elif isinstance(r, Exception):
                logger.debug(f"Image download error: {r}")
        return saved

    def _link_type(self, url: str) -> str:
        """Phân loại link: 'video' (YouTube, Vimeo, file media) hoặc 'page' (trang web)."""
        if not url:
            return "unknown"
        u = url.lower().strip()
        video_patterns = (
            "youtube.com", "youtu.be", "vimeo.com", "dailymotion.com",
            ".mp4", ".webm", ".ogg", ".m3u8", "video/", "embed",
            "drive.google.com/file", "fb.watch", "facebook.com/watch",
        )
        for p in video_patterns:
            if p in u:
                return "video"
        return "page"

    def _collect_all_links_from_content(self) -> set[str]:
        """Gom tất cả link (link + video_links) từ content_data, trả về set URL unique."""
        out = set()
        for item in self.content_data:
            for key in ("link", "video_links"):
                val = item.get(key)
                if isinstance(val, list):
                    for u in val:
                        if u and isinstance(u, str) and u.startswith("http"):
                            out.add(u.strip())
                elif isinstance(val, str) and val.strip().startswith("http"):
                    out.add(val.strip())
        return out

    async def _summarize_one_link(
        self, url: str, crawler, semaphore
    ) -> dict | None:
        """
        Truy cập link và tóm tắt: type (video/page), title, summary (đoạn ngắn).
        Video: không fetch, chỉ ghi nhận nguồn (YouTube, Vimeo, ...).
        Page: fetch lấy title + ~300 ký tự đầu nội dung.
        """
        try:
            link_type = self._link_type(url)
            if link_type == "video":
                if "youtube.com" in url.lower() or "youtu.be" in url.lower():
                    platform = "YouTube"
                elif "vimeo.com" in url.lower():
                    platform = "Vimeo"
                elif "drive.google.com" in url.lower():
                    platform = "Google Drive"
                else:
                    platform = "Video/Media"
                return {
                    "url": url,
                    "type": "video",
                    "title": platform,
                    "summary": f"Link video/media: {url[:120]}{'...' if len(url) > 120 else ''}",
                }

            async with semaphore:
                result = await crawler.arun(
                    url=url,
                    bypass_cache=True,
                    wait_until="domcontentloaded",
                )
            if not result or not getattr(result, "success", False):
                return {
                    "url": url,
                    "type": "page",
                    "title": "(không lấy được)",
                    "summary": "Truy cập thất bại hoặc timeout.",
                }
            html = getattr(result, "html", "") or ""
            md = getattr(result, "markdown", "") or ""
            title = self.extract_title(md) or self.extract_title_from_html(html) or "(không có tiêu đề)"
            text = (md or html).replace("\n", " ").strip()
            text = re.sub(r"\s+", " ", text)[:400].strip()
            if len((md or html)) > 400:
                text += "..."
            return {
                "url": url,
                "type": "page",
                "title": self._clean_text(title),
                "summary": self._clean_text(text) or "(không có nội dung)",
            }
        except Exception as e:
            logger.debug(f"Summarize {url[:60]}: {e}")
            return {
                "url": url,
                "type": "unknown",
                "title": "",
                "summary": f"Lỗi: {str(e)[:200]}",
            }

    async def fetch_and_summarize_links(
        self,
        output_file: str = "link_summaries.csv",
        max_links: int | None = None,
        concurrency: int = 5,
    ) -> list[dict]:
        """
        Thu thập mọi link từ content_data (link + video_links), truy cập từng link,
        tóm tắt dữ liệu (video: ghi nguồn; page: title + đoạn nội dung ngắn), lưu CSV.
        """
        all_urls = self._collect_all_links_from_content()
        if not all_urls:
            logger.warning("Không có link nào trong content_data để tóm tắt.")
            return []
        if max_links is not None:
            all_urls = set(list(all_urls)[: max_links])
        urls = list(all_urls)
        logger.info(f"Bắt đầu tóm tắt {len(urls)} link (video + page)...")

        semaphore = asyncio.Semaphore(concurrency)
        summaries = []

        try:
            async with AsyncWebCrawler() as crawler:
                tasks = [
                    self._summarize_one_link(u, crawler, semaphore)
                    for u in urls
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for r in results:
                    if isinstance(r, Exception):
                        logger.error(f"Link summary error: {r}")
                        continue
                    if r:
                        summaries.append(r)
        except Exception as e:
            logger.error(f"fetch_and_summarize_links: {e}")
            return summaries

        if summaries:
            df = pd.DataFrame(summaries)
            df.to_csv(output_file, index=False, encoding="utf-8-sig")
            logger.info(f"Đã lưu tóm tắt {len(summaries)} link -> {output_file}")
        return summaries

    def extract_title(self, markdown: str) -> str:
        """Extract title from markdown (# header)"""
        lines = markdown.split("\n")
        for line in lines:
            line = line.strip()
            if line.startswith("#") and len(line) > 1:
                return line.lstrip("#").strip()
        return ""

    def extract_title_from_html(self, html: str) -> str:
        """Extract title from HTML if markdown doesn't have it"""
        import re

        # Look for <h1>, <h2>, <title> tags
        patterns = [
            r"<h1[^>]*>([^<]+)</h1>",
            r"<title[^>]*>([^<]+)</title>",
            r"<h2[^>]*>([^<]+)</h2>",
        ]
        for pattern in patterns:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        return ""

    def extract_navigation(self, html: str) -> str:
        """Extract navigation/breadcrumb (flat)"""
        navigation = []
        breadcrumb = re.search(
            r"<nav[^>]*>.*?<ol|<ul[^>]*>(.*?)</ol>|</ul>",
            html,
            re.DOTALL | re.IGNORECASE,
        )
        if breadcrumb:
            links = re.findall(
                r'<a[^>]*href=[\'"]([^\'"]*)[\'"][^>]*>([^<]+)</a>',
                breadcrumb.group(0),
                re.IGNORECASE,
            )
            for href, text in links:
                navigation.append(text.strip())
        return " | ".join(navigation) if navigation else "Home"

    def extract_navigation_tree(self, html: str, base_url: str) -> str:
        """Trang home: navigation dạng tree node (cấp cha-con bằng indent)"""
        tree_lines = ["Home"]
        try:
            # Menu chính: nav hoặc header chứa ul > li
            nav_blocks = re.findall(
                r'<nav[^>]*>(.*?)</nav>|<header[^>]*>.*?<nav[^>]*>(.*?)</nav>',
                html,
                re.DOTALL | re.IGNORECASE,
            )
            for block in nav_blocks:
                fragment = (block[0] or block[1]) if isinstance(block, tuple) else block
                if not fragment or not fragment.strip():
                    continue
                fragment = fragment.strip()
                # ul > li (cấp 1)
                li_matches = re.findall(
                    r'<li[^>]*>(.*?)</li>',
                    fragment,
                    re.DOTALL | re.IGNORECASE,
                )
                for li in li_matches:
                    a = re.search(r'<a[^>]*href=[\'"]([^\'"]*)[\'"][^>]*>([^<]+)</a>', li, re.IGNORECASE)
                    if a:
                        label = re.sub(r"<[^>]+>", "", a.group(2)).strip()
                        if label:
                            tree_lines.append(f"  └─ {label}")
                    # submenu: ul trong li
                    sub_ul = re.search(r'<ul[^>]*>(.*?)</ul>', li, re.DOTALL | re.IGNORECASE)
                    if sub_ul:
                        sub_lis = re.findall(r'<li[^>]*>.*?<a[^>]*>([^<]+)</a>', sub_ul.group(1), re.IGNORECASE)
                        for sl in sub_lis:
                            tree_lines.append(f"      └─ {sl.strip()}")
        except Exception as e:
            logger.debug(f"extract_navigation_tree: {e}")
        return "\n".join(tree_lines) if len(tree_lines) > 1 else "Home"

    def _parse_nav_to_path(self, nav_str: str) -> list[tuple[int, str]]:
        """Parse chuỗi navigation thành list (level, label) để xây cây id/parent."""
        path = []
        if not nav_str or not str(nav_str).strip():
            return [(0, "Home")]
        s = str(nav_str).strip()
        if s == "Home":
            return [(0, "Home")]
        # Dạng tree: "Home\n  └─ A\n      └─ B"
        lines = s.split("\n")
        for line in lines:
            line = line.rstrip()
            if not line:
                continue
            if line.strip() == "Home":
                path.append((0, "Home"))
                continue
            if "└─" in line:
                rest = line.split("└─", 1)[-1].strip()
                if not rest:
                    continue
                # Mức thụt vào: 2 spaces = level 1, 6 = level 2, ...
                spaces = len(line) - len(line.lstrip())
                level = max(0, (spaces // 2) if spaces > 0 else 1)
                path.append((level, rest))
            else:
                break
        # Dạng flat: "A | B | C"
        if not path and "|" in s:
            for i, part in enumerate(s.split("|")):
                path.append((i, part.strip() or f"Level{i}"))
            return path
        if not path and s:
            path.append((0, s))
        return path if path else [(0, "Home")]

    def _build_nav_tree_and_assign_ids(self) -> list[dict]:
        """
        Gom mọi navigation thành cây (id, parent_id, label, level).
        Gán nav_id cho từng item trong content_data (id node lá của trang đó).
        Trả về list node: [{id, parent_id, label, level}].
        """
        # (label, parent_id) -> id để tránh trùng
        key_to_id: dict[tuple[str, int], int] = {}
        nodes: list[dict] = []
        next_id = 1
        root_id = next_id
        nodes.append({"id": root_id, "parent_id": 0, "label": "Home", "level": 0})
        key_to_id[("Home", 0)] = root_id
        next_id += 1

        for item in self.content_data:
            path = self._parse_nav_to_path(item.get("navigation") or "Home")
            parent_id = root_id
            for level, label in path:
                if not label:
                    continue
                key = (label, parent_id)
                if key in key_to_id:
                    nid = key_to_id[key]
                else:
                    nid = next_id
                    next_id += 1
                    key_to_id[key] = nid
                    nodes.append({"id": nid, "parent_id": parent_id, "label": label, "level": level})
                parent_id = nid
            item["nav_id"] = parent_id

        return nodes

    def save_nav_only(self, output_dir: str, filename: str = "navigation.json") -> str | None:
        """Lưu 1 file duy nhất chứa cây nav (id, parent_id, label, level). Các file page không cần lưu nav."""
        if not self.content_data:
            return None
        try:
            nav_nodes = self._build_nav_tree_and_assign_ids()
            os.makedirs(output_dir, exist_ok=True)
            path = os.path.join(output_dir, filename)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(nav_nodes, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved nav tree ({len(nav_nodes)} nodes) -> {path}")
            return path
        except Exception as e:
            logger.error(f"save_nav_only: {e}")
            return None

    def export_all_to_one_json_and_insert(
        self, output_dir: str, json_filename: str = "all_data.json", sql_filename: str = "storage_import.sql"
    ) -> list[str]:
        """
        Gộp toàn bộ data thành 1 file JSON (all_data.json) và sinh file SQL INSERT (storage_import.sql).
        JSON: [ { "id", "title", "content", "created_at", "video_links", "image_urls" }, ... ]
        SQL: INSERT vào storage_data, storage_video, storage_image.
        """
        if not self.content_data:
            logger.warning("No content_data to export")
            return []
        os.makedirs(output_dir, exist_ok=True)
        written = []

        # Gộp thành list 1 page = 1 object (có id, video_links, image_urls)
        rows_data = []
        rows_video = []
        rows_image = []

        for item in self.content_data:
            storage_id = uuid.uuid4()
            title = self._clean_text(item.get("title", "")) or "(no title)"
            content = item.get("content", "") or ""
            created_at = item.get("timestamp", "") or datetime.now().isoformat()
            link = (item.get("url") or "").strip()[:2000]  # đường dẫn trang (storage_data.link)
            video_links = [v.strip()[:500] for v in (item.get("video_links") or []) if v and isinstance(v, str) and v.strip()]
            image_urls = [img.strip()[:500] for img in (item.get("image_urls") or []) if img and isinstance(img, str) and img.strip()]

            rows_data.append({
                "id": str(storage_id),
                "title": title,
                "content": content,
                "created_at": created_at,
                "link": link,
                "video_links": video_links,
                "image_urls": image_urls,
            })
            for v in video_links:
                rows_video.append({
                    "id": str(uuid.uuid4()),
                    "storage_id": str(storage_id),
                    "video_url": v,
                })
            for u in image_urls:
                rows_image.append({
                    "id": str(uuid.uuid4()),
                    "storage_id": str(storage_id),
                    "image_url": u,
                })

        # 1 file JSON gộp tất cả (để đọc / backup / insert sau)
        json_path = os.path.join(output_dir, json_filename)
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(rows_data, f, ensure_ascii=False, indent=2)
        written.append(json_path)
        logger.info(f"Saved 1 file JSON: {len(rows_data)} pages -> {json_path}")

        # SQL INSERT từ cùng dữ liệu
        def esc(s: str) -> str:
            return (s or "").replace("\\", "\\\\").replace("'", "''")

        sql_path = os.path.join(output_dir, sql_filename)
        with open(sql_path, "w", encoding="utf-8") as f:
            f.write("-- Generated from all_data.json\n")
            f.write("-- storage_data\n")
            for r in rows_data:
                c = esc(r["content"])[:50000]
                t = esc(r["title"])
                lnk = esc(r.get("link", ""))[:2000]
                f.write(f"INSERT INTO storage_data (id, title, content, created_at, link) VALUES ('{r['id']}', '{t}', '{c}', '{r['created_at']}', '{lnk}');\n")
            f.write("\n-- storage_video\n")
            for r in rows_video:
                v = esc(r["video_url"])
                f.write(f"INSERT INTO storage_video (id, storage_id, video_url) VALUES ('{r['id']}', '{r['storage_id']}', '{v}');\n")
            f.write("\n-- storage_image\n")
            for r in rows_image:
                u = esc(r["image_url"])
                f.write(f"INSERT INTO storage_image (id, storage_id, image_url) VALUES ('{r['id']}', '{r['storage_id']}', '{u}');\n")
        written.append(sql_path)
        logger.info(f"Saved SQL INSERT: {len(rows_data)} data, {len(rows_video)} video, {len(rows_image)} image -> {sql_path}")

        return written

    def export_for_storage_tables(self, output_dir: str) -> list[str]:
        """
        Gộp 1 file JSON + sinh SQL INSERT. Gọi export_all_to_one_json_and_insert.
        Giữ tên cũ để tương thích.
        """
        return self.export_all_to_one_json_and_insert(
            output_dir,
            json_filename="all_data.json",
            sql_filename="storage_import.sql",
        )

    def save_as_markdown(self, filename: str = "benhvien_content_detailed.md"):
        """Save extracted content as markdown with title + content format"""
        if not self.content_data:
            logger.warning("No content to save")
            return

        try:
            with open(filename, "w", encoding="utf-8") as f:
                for idx, item in enumerate(self.content_data, 1):
                    f.write(f"\n{'='*80}\n")
                    f.write(f"# {item['title']}\n\n")
                    f.write(f"**Navigation:**\n{item['navigation']}\n\n")
                    f.write(f"**URL:** {item['url']}\n")
                    f.write(f"**Timestamp:** {item['timestamp']}\n\n")
                    f.write(f"---\n\n")
                    f.write(f"{item['content']}\n\n")

            logger.info(f"Saved {len(self.content_data)} pages to {filename}")
        except Exception as e:
            logger.error(f"Error saving markdown: {e}")

    def save_as_json(self, filename: str = "benhvien_content_detailed.json"):
        """Save as JSON for structured access"""
        if not self.content_data:
            logger.warning("No content to save")
            return

        try:
            import json

            with open(filename, "w", encoding="utf-8") as f:
                json.dump(self.content_data, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved {len(self.content_data)} pages to {filename}")
        except Exception as e:
            logger.error(f"Error saving JSON: {e}")

    def save_as_csv(self, filename: str = "benhvien_crawled_data.csv"):
        """Xuất dữ liệu đã crawl (từ các link trong CSV) ra file CSV."""
        if not self.content_data:
            logger.warning("No content to save")
            return
        try:
            rows = []
            for item in self.content_data:
                rows.append({
                    "title": self._clean_text(item.get("title", "")),
                    "url": (item.get("url", "") or "").strip(),
                    "content": self._clean_content(item.get("content", "")),
                    "video_links": self._links_to_column(item.get("video_links", [])),
                    "navigation": self._clean_text(item.get("navigation", "")),
                    "timestamp": item.get("timestamp", ""),
                })
            df = pd.DataFrame(rows)
            df.to_csv(filename, index=False, encoding="utf-8-sig")
            logger.info(f"Saved {len(self.content_data)} rows to {filename}")
        except Exception as e:
            logger.error(f"Error saving CSV: {e}")

    def save_as_xlsx(self, filename: str = "benhvien_content_detailed.xlsx"):
        """
        Lưu Excel nhiều sheet:
        - Sheet Navigation: id, parent_id, label, level (cây nav để quản lý).
        - Sheet Content: title, url, nav_id, content_preview, full_content, video_links, timestamp.
        """
        if not self.content_data:
            logger.warning("No content to save")
            return

        try:
            nav_nodes = self._build_nav_tree_and_assign_ids()

            df_nav = pd.DataFrame(nav_nodes)
            # Đảm bảo thứ tự cột: id, parent_id, label, level
            df_nav = df_nav[["id", "parent_id", "label", "level"]]

            content_rows = []
            for item in self.content_data:
                raw = item.get("content", "")
                content_rows.append({
                    "title": self._clean_text(item.get("title", "")),
                    "url": (item.get("url", "") or "").strip(),
                    "nav_id": item.get("nav_id", ""),
                    "navigation": self._clean_text(item.get("navigation", "")),
                    "video_links": self._links_to_column(item.get("video_links", [])),
                    "content_preview": self._clean_content((raw[:500] + "...") if len(raw) > 500 else raw),
                    "full_content": self._clean_content(raw),
                    "timestamp": item.get("timestamp", ""),
                })
            df_content = pd.DataFrame(content_rows)

            with pd.ExcelWriter(filename, engine="openpyxl") as writer:
                df_nav.to_excel(writer, sheet_name="Navigation", index=False)
                df_content.to_excel(writer, sheet_name="Content", index=False)

            logger.info(
                f"Saved Excel: sheet Navigation ({len(df_nav)} nodes), sheet Content ({len(self.content_data)} rows) -> {filename}"
            )
        except Exception as e:
            logger.error(f"Error saving XLSX: {e}")

    def _slug(self, label: str) -> str:
        """Tên file an toàn từ label (bỏ ký tự đặc biệt)."""
        s = re.sub(r"[^\w\s-]", "", label)
        s = re.sub(r"[-\s]+", "_", s).strip("_")
        return s or "unnamed"

    def _page_slug(self, url: str, title: str = "") -> str:
        """Tên thư mục riêng cho từng page (từ URL path + title + hash để tránh trùng)."""
        p = urlparse(url)
        path = (p.path or "/").strip("/")
        path_slug = re.sub(r"[^\w\-/]", "_", path)
        path_slug = re.sub(r"_+", "_", path_slug).strip("_") or "page"
        if len(path_slug) > 50:
            path_slug = path_slug[:50]
        title_slug = self._slug(title)[:30] if title else ""
        unique = hashlib.sha256(url.encode()).hexdigest()[:8]
        if title_slug:
            return f"{path_slug}_{title_slug}_{unique}"
        return f"{path_slug}_{unique}"

    def _clean_text(self, s: str) -> str:
        """Clean chuỗi: bỏ khoảng trắng thừa, decode HTML entity, strip."""
        if not s or not isinstance(s, str):
            return ""
        s = s.strip()
        s = re.sub(r"\s+", " ", s)
        s = s.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", '"')
        return s.strip()

    def _clean_content(self, s: str) -> str:
        """Clean nội dung: gộp dòng trống liên tiếp, trim từng dòng."""
        if not s or not isinstance(s, str):
            return ""
        lines = [ln.strip() for ln in s.splitlines()]
        out = []
        prev_blank = False
        for ln in lines:
            is_blank = not ln
            if is_blank and prev_blank:
                continue
            prev_blank = is_blank
            out.append(ln)
        return "\n".join(out).strip()

    def _strip_urls_from_content(self, text: str) -> str:
        """Bỏ URL khỏi content (link/video/ảnh đã lưu riêng cột, content chỉ giữ chữ)."""
        if not text or not isinstance(text, str):
            return ""
        # Markdown link [text](url) -> chỉ giữ text
        text = re.sub(r'\[([^\]]*)\]\(https?://[^\)]+\)', r'\1', text)
        # URL trần: https?://...
        text = re.sub(r'https?://\S+', '', text)
        # Gộp nhiều space trên cùng dòng (giữ xuống dòng)
        text = re.sub(r'[^\S\n]+', ' ', text).strip()
        return text

    def _links_to_column(self, links: list | str) -> str:
        """Chuyển list link thành một cột: mỗi link một dòng (dễ quản lý trong CSV/Excel)."""
        if isinstance(links, str):
            return links.strip() if links else ""
        if not links:
            return ""
        return "\n".join(str(u).strip() for u in links if u)

    def save_csv_by_label(self, output_dir: str = "csv_by_label") -> list[str]:
        """
        Chia dữ liệu đã crawl theo label → mỗi label một file CSV riêng.
        Cần đã gọi load_links_from_markdown() và crawl xong (content_data + _label_to_urls).
        Trả về list đường dẫn file đã ghi.
        """
        if not getattr(self, "_label_to_urls", None) or not self._label_to_urls:
            logger.warning("No _label_to_urls (chỉ có khi đọc từ file * [Label](URL))")
            return []
        url_to_item = {item["url"]: item for item in self.content_data}

        os.makedirs(output_dir, exist_ok=True)
        written = []
        for label, urls in self._label_to_urls.items():
            rows = []
            for url in urls:
                if url in url_to_item:
                    item = url_to_item[url]
                    rows.append({
                        "label": self._clean_text(label),
                        "title": self._clean_text(item.get("title", "")),
                        "url": (item.get("url", "") or "").strip(),
                        "nav_id": item.get("nav_id", ""),
                        "content": self._clean_content(item.get("content", "")),
                        "video_links": self._links_to_column(item.get("video_links", [])),
                        "navigation": self._clean_text(item.get("navigation", "")),
                        "timestamp": item.get("timestamp", ""),
                    })
            if not rows:
                logger.debug(f"Label '{label}' không có dữ liệu crawl (URL chưa crawl được)")
                continue
            df = pd.DataFrame(rows)
            safe_name = self._slug(label)
            path = os.path.join(output_dir, f"{safe_name}.csv")
            df.to_csv(path, index=False, encoding="utf-8-sig")
            written.append(path)
            logger.info(f"  -> {path} ({len(rows)} rows)")
        logger.info(f"Saved {len(written)} CSV files in {output_dir}/")
        return written

    def save_csv_by_nav(self, output_dir: str = "csv_by_nav") -> list[str]:
        """
        Chia dữ liệu đã crawl theo navigation (nav_id) → mỗi nav một file CSV nhỏ để quản lý.
        Cần đã crawl xong và có content_data. Sẽ gọi _build_nav_tree_and_assign_ids().
        """
        if not self.content_data:
            logger.warning("No content to save")
            return []
        try:
            nav_nodes = self._build_nav_tree_and_assign_ids()
            id_to_label = {n["id"]: n["label"] for n in nav_nodes}
        except Exception as e:
            logger.error(f"Build nav tree: {e}")
            return []

        by_nav: dict[int, list[dict]] = {}
        for item in self.content_data:
            nid = item.get("nav_id") or 1
            by_nav.setdefault(nid, []).append(item)

        os.makedirs(output_dir, exist_ok=True)
        written = []
        for nid, items in by_nav.items():
            label = id_to_label.get(nid, f"nav_{nid}")
            rows = []
            for item in items:
                rows.append({
                    "title": self._clean_text(item.get("title", "")),
                    "url": (item.get("url", "") or "").strip(),
                    "page_folder": item.get("page_folder", ""),
                    "nav_id": nid,
                    "content": self._clean_content(item.get("content", "")),
                    "video_links": self._links_to_column(item.get("video_links", [])),
                    "image_urls": self._links_to_column(item.get("image_urls", [])),
                    "timestamp": item.get("timestamp", ""),
                })
            safe = self._slug(label)
            path = os.path.join(output_dir, f"{safe}.csv")
            pd.DataFrame(rows).to_csv(path, index=False, encoding="utf-8-sig")
            written.append(path)
            logger.info(f"  -> {path} ({len(rows)} rows)")
        logger.info(f"Saved {len(written)} CSV files (theo nav) in {output_dir}/")
        return written

    def save_all_pages_one_row(self, output_dir: str, filename: str = "all_pages.csv") -> str | None:
        """
        Lưu 1 file CSV: 1 page = 1 dòng (mỗi trang crawl là đúng 1 dòng data).
        Cột: title, url, nav_id, page_folder, content, video_links, image_urls, timestamp.
        """
        if not self.content_data:
            return None
        try:
            nav_nodes = self._build_nav_tree_and_assign_ids()
            id_to_label = {n["id"]: n["label"] for n in nav_nodes}
        except Exception:
            id_to_label = {}

        rows = []
        for item in self.content_data:
            nid = item.get("nav_id") or 1
            rows.append({
                "title": self._clean_text(item.get("title", "")),
                "url": (item.get("url", "") or "").strip(),
                "nav_id": nid,
                "page_folder": item.get("page_folder", ""),
                "content": self._clean_content(item.get("content", "")),
                "video_links": self._links_to_column(item.get("video_links", [])),
                "image_urls": self._links_to_column(item.get("image_urls", [])),
                "timestamp": item.get("timestamp", ""),
            })
        os.makedirs(output_dir, exist_ok=True)
        path = os.path.join(output_dir, filename)
        pd.DataFrame(rows).to_csv(path, index=False, encoding="utf-8-sig")
        logger.info(f"Saved 1 page = 1 row: {len(rows)} rows -> {path}")
        return path

    async def run(self):
        """Run the content fetcher (asyncio song song, giới hạn bởi concurrency)."""
        if not self.load_urls_from_csv():
            return

        # Init state (resume) cho chế độ crawl theo CSV
        self._set_state(output_root=".", events_filename="events_db.csvmode.jsonl")
        enqueued, started, done_ok, done_failed = (set(), set(), set(), set())
        if self.resume and self._state:
            enqueued, started, done_ok, done_failed = self._state.load()
            # visited_urls dùng canonical URL
            self.visited_urls = set(done_ok)

        to_crawl_all = [
            (url, title_csv)
            for url, title_csv in self.urls_to_process
            if self.is_same_domain(url)
        ]

        # Filter theo resume
        to_crawl = []
        for url, title_csv in to_crawl_all:
            canon = self._canonical_url(url)
            if canon in done_ok:
                continue
            if (not self.retry_failed_on_resume) and (canon in done_failed):
                continue
            to_crawl.append((url, title_csv))

        if self._state and self.resume:
            for url, _ in to_crawl:
                await self._state.record("enqueue", self._canonical_url(url))

        if not to_crawl:
            logger.warning("No URLs to crawl (same domain).")
            return

        self._semaphore = asyncio.Semaphore(self.concurrency)
        csv_path = "benhvien_crawled_data.csv"
        jsonl_path = "benhvien_crawled_data.jsonl"

        try:
            # Tạo file CSV + JSONL ngay, ghi header CSV
            self._csv_file = open(csv_path, "w", encoding="utf-8-sig", newline="")
            self._csv_writer = csv.writer(self._csv_file)
            self._csv_writer.writerow(["title", "url", "nav_id", "content", "video_links", "image_urls", "timestamp"])
            self._csv_file.flush()
            self._jsonl_file = open(jsonl_path, "w", encoding="utf-8")

            async with AsyncWebCrawler() as crawler:
                self.crawler = crawler

                tasks = [
                    self._fetch_one(url, title_from_csv=title_csv)
                    for url, title_csv in to_crawl
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for r in results:
                    if isinstance(r, Exception):
                        logger.error(f"Task error: {r}")

            # Đóng file stream (đã ghi dần từng kết quả)
            if self._csv_file:
                self._csv_file.close()
                self._csv_file = None
            if self._jsonl_file:
                self._jsonl_file.close()
                self._jsonl_file = None

            # Ghi thêm file JSON dạng mảng (từ content_data đã gom)
            self.save_as_json("benhvien_crawled_data.json")
            self.save_as_markdown("benhvien_content_detailed.md")
            self.save_as_xlsx("benhvien_content_detailed.xlsx")

            print(f"\n=== Crawl4AI Content Fetcher Summary ===")
            print(f"Input: link trong file CSV → crawl từng link → ghi ngay từng kết quả.")
            print(f"Total pages processed: {len(self.content_data)}")
            print(f"Total URLs visited: {len(self.visited_urls)}")
            print(f"Concurrency: {self.concurrency} (asyncio parallel)")
            print(f"\nFiles (ghi dần khi crawl xong từng trang):")
            print(f"  - benhvien_crawled_data.csv (CSV, ghi ngay)")
            print(f"  - benhvien_crawled_data.jsonl (JSON Lines, ghi ngay)")
            print(f"  - benhvien_crawled_data.json (JSON mảng, ghi cuối)")
            print(f"  - benhvien_content_detailed.md, .xlsx")

        except Exception as e:
            logger.error(f"Fatal error: {e}")
        finally:
            if getattr(self, "_csv_file", None):
                try:
                    self._csv_file.close()
                except Exception:
                    pass
                self._csv_file = None
            if getattr(self, "_jsonl_file", None):
                try:
                    self._jsonl_file.close()
                except Exception:
                    pass
                self._jsonl_file = None

    def is_same_domain(self, url: str) -> bool:
        """Check if URL belongs to the same domain"""
        try:
            parsed = urlparse(url)
            return (self.base_domain or "") in (parsed.netloc or "")
        except Exception:
            return False

    async def run_from_seed_url(
        self,
        seed_url: str,
        output_dir: str = "csv_by_nav",
        max_pages: int | None = 500,
        concurrency: int = 10,
        summarize_links: bool = True,
        max_summary_links: int | None = 300,
        download_images: bool = False,
    ):
        """
        Vào web từ URL gốc, crawl toàn bộ trang (kể cả trang con), trích raw_data + link video/ảnh.
        Ảnh và video chỉ lưu URL vào cột riêng (image_urls, video_links), không tải file.

        - seed_url: URL bắt đầu (vd: https://benhvienanbinh.vn/)
        - download_images: False (mặc định) = chỉ lưu URL ảnh/video vào cột; True = tải ảnh về folder
        - summarize_links: truy cập từng link (video/page) và tóm tắt -> link_summaries.csv
        """
        self.base_domain = self._domain_from_url(seed_url)
        if not self.base_domain:
            logger.error("Không xác định được domain từ seed URL")
            return
        self.home_url = self.normalize_path(seed_url)
        self.visited_urls = set()
        self.content_data = []
        self._nav_key_to_id = {}
        self._nav_next_id = 2
        # Mỗi page một folder riêng: output_dir/pages/{page_slug}/ (data.json, content.md, images/)
        self._pages_dir = os.path.join(output_dir, "pages")
        self._images_dir = None  # không dùng thư mục ảnh chung khi đã dùng folder từng page
        self._download_images = bool(download_images)

        # Init state (resume) cho chế độ seed crawl
        self._set_state(output_root=output_dir, events_filename="events_db.seedmode.jsonl")
        enqueued, started, done_ok, done_failed = (set(), set(), set(), set())
        if self.resume and self._state:
            enqueued, started, done_ok, done_failed = self._state.load()

        to_crawl: set[str] = set()
        visited_norm = set(done_ok) if self.resume else set()
        self.visited_urls = set(done_ok) if self.resume else set()

        seed_canon = self._canonical_url(seed_url)
        if self.resume and self._state:
            # Resume đúng pending queue: (enqueued | started) - done_ok
            pending = (set(enqueued) | set(started)) - set(done_ok)
            if not self.retry_failed_on_resume:
                pending -= set(done_failed)

            if seed_canon not in set(enqueued) and seed_canon not in set(done_ok):
                pending.add(seed_canon)
                await self._state.record("enqueue", seed_canon)

            for u in pending:
                if u not in visited_norm:
                    to_crawl.add(u)

            logger.info(
                f"Resume enabled (seed mode): done_ok={len(done_ok)}, done_failed={len(done_failed)}, pending={len(to_crawl)}"
            )
        else:
            if seed_canon not in visited_norm:
                to_crawl.add(seed_canon)
                if self._state and self.resume:
                    await self._state.record("enqueue", seed_canon)
        self._semaphore = asyncio.Semaphore(concurrency or self.concurrency)

        try:
            async with AsyncWebCrawler() as crawler:
                self.crawler = crawler

                while to_crawl and (max_pages is None or len(self.content_data) < max_pages):
                    batch = []
                    batch_norm = set()
                    for u in list(to_crawl):
                        u_n = self.normalize_path(u)
                        if u_n in visited_norm or u_n in batch_norm:
                            continue
                        batch_norm.add(u_n)
                        batch.append(u)
                        if len(batch) >= (concurrency or 10):
                            break
                    if not batch:
                        break
                    for u in batch:
                        visited_norm.add(self.normalize_path(u))
                    to_crawl -= set(batch)

                    tasks = [self._fetch_one_for_seed(u) for u in batch]
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    for r in results:
                        if isinstance(r, Exception):
                            logger.error(f"Task error: {r}")
                            continue
                        if r:
                            self._assign_nav_id_incremental(r)
                            self.content_data.append(r)
                            for u in r.get("internal_links") or []:
                                if not u or not self.is_same_domain(u):
                                    continue
                                u_norm = self.normalize_path(u)
                                if u_norm not in visited_norm:
                                    to_crawl.add(u)
                                    if self._state and self.resume:
                                        await self._state.record("enqueue", u_norm)

            written = self.save_csv_by_nav(output_dir=output_dir)
            one_row_path = self.save_all_pages_one_row(output_dir, "all_pages.csv")
            if one_row_path:
                written.append(one_row_path)
            nav_path = self.save_nav_only(output_dir, "navigation.json")
            if nav_path:
                written.append(nav_path)
            # Định dạng sẵn cho 3 bảng: storage_data, storage_video, storage_image
            storage_files = self.export_for_storage_tables(output_dir)
            written.extend(storage_files)
            if summarize_links and self.content_data:
                summary_path = os.path.join(output_dir, "link_summaries.csv")
                await self.fetch_and_summarize_links(
                    output_file=summary_path,
                    max_links=max_summary_links,
                    concurrency=5,
                )
                written.append(summary_path)
            print(f"\n=== Crawl toàn site từ URL gốc ===")
            print(f"Seed: {seed_url}")
            print(f"Domain: {self.base_domain}")
            print(f"Tổng trang đã crawl: {len(self.content_data)}")
            print(f"Đã chia thành {len(written)} file trong: {output_dir}/")
            print(f"  Import DB: storage_data.csv, storage_video.csv, storage_image.csv, storage_import.sql")
            if getattr(self, "_pages_dir", None) and os.path.isdir(self._pages_dir):
                n_pages = len([d for d in os.listdir(self._pages_dir) if os.path.isdir(os.path.join(self._pages_dir, d))])
                print(f"  Data + ảnh từng page: {n_pages} folder trong {self._pages_dir}/ (mỗi folder: data.json, content.md, images/)")
            for p in written[:25]:
                print(f"  - {p}")
            if len(written) > 25:
                print(f"  ... và {len(written) - 25} file khác")
        except Exception as e:
            logger.error(f"Fatal error: {e}")

    async def _fetch_one_for_seed(self, url: str) -> dict | None:
        """Crawl 1 URL (dùng trong run_from_seed_url), không ghi file stream."""
        canon = self._canonical_url(url)
        if canon in self.visited_urls:
            return None
        self.visited_urls.add(canon)
        async with self._semaphore:
            logger.info(f"Fetching: {url} (đã crawl {len(self.content_data)} trang)")
            if self._state and self.resume:
                await self._state.record("start", canon)
            try:
                result = await self._do_fetch(url, title_from_csv=None)
                if result:
                    await self._push_to_db(result)
                    if self._state and self.resume:
                        await self._state.record("done", canon, ok=True)
                else:
                    if self._state and self.resume:
                        await self._state.record("done", canon, ok=False, error="fetch_failed")
                return result
            except Exception as e:
                err = str(e)
                logger.error(f"Fetch error {url}: {err}")
                if self._state and self.resume:
                    await self._state.record("done", canon, ok=False, error=err)
                return None

    async def run_from_links_file(
        self,
        links_file: str,
        output_dir: str = "csv_by_label",
        concurrency: int = 10,
    ):
        """
        Đọc file link dạng * [Label](URL), crawl từng URL, chia thành các CSV riêng theo label.
        Ví dụ: links.txt chứa
          * [Quá trình hình thành & phát triển](https://...)
          * [Ban Giám đốc](https://...)
        → crawl xong sẽ có csv_by_label/Qua_trinh_hinh_thanh_phat_trien.csv, Ban_Giam_doc.csv, ...
        """
        if not self.load_links_from_markdown(links_file):
            return

        to_crawl = [
            (url, label)
            for url, label in self.urls_to_process
            if self.is_same_domain(url)
        ]
        if not to_crawl:
            logger.warning("No URLs to crawl (same domain).")
            return

        self._semaphore = asyncio.Semaphore(concurrency or self.concurrency)
        self._set_state(output_root=output_dir, events_filename="events_db.linksmode.jsonl")
        if self.resume and self._state:
            _, _, done_ok, done_failed = self._state.load()
            self.visited_urls = set(done_ok)
            filtered = []
            for url, label in to_crawl:
                canon = self._canonical_url(url)
                if canon in done_ok:
                    continue
                if (not self.retry_failed_on_resume) and (canon in done_failed):
                    continue
                filtered.append((url, label))
            to_crawl = filtered
            for url, _ in to_crawl:
                await self._state.record("enqueue", self._canonical_url(url))

        try:
            async with AsyncWebCrawler() as crawler:
                self.crawler = crawler
                tasks = [
                    self._fetch_one(url, title_from_csv=label)
                    for url, label in to_crawl
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for r in results:
                    if isinstance(r, Exception):
                        logger.error(f"Task error: {r}")

            written = self.save_csv_by_label(output_dir=output_dir)
            print(f"\n=== Crawl từ link dạng * [Label](URL) ===")
            print(f"File link: {links_file}")
            print(f"Total pages: {len(self.content_data)}")
            print(f"Đã chia thành {len(written)} CSV trong thư mục: {output_dir}/")
            for p in written:
                print(f"  - {p}")
        except Exception as e:
            logger.error(f"Fatal error: {e}")


async def main():
    # Chế độ 1: Vào web từ URL gốc → crawl toàn bộ site (trang con) → chia CSV theo nav + trích video
    scraper = CrawlAIBenhVienScraperSimple(max_pages=None, concurrency=10)
    await scraper.run_from_seed_url(
        seed_url="https://benhvienanbinh.vn/",  # URL web cần crawl
        output_dir="csv_by_nav",
        max_pages=500,   # None = không giới hạn
        concurrency=10,
        summarize_links=True,      # Truy cập từng link (video/page) và tóm tắt
        max_summary_links=300,     # Giới hạn số link tóm tắt (None = tất cả)
        download_images=False,    # Chỉ lưu URL ảnh/video vào cột (True = tải ảnh về folder)
    )

    # Chế độ 2: CSV có cột link/url (như cũ)
    # scraper = CrawlAIBenhVienScraperSimple(input_csv="benhvien_data.csv", max_pages=None, concurrency=10)
    # await scraper.run()

    # Chế độ 3: File link dạng * [Label](URL) → crawl và chia mỗi label một CSV riêng
    # await scraper.run_from_links_file(links_file="links.txt", output_dir="csv_by_label")


if __name__ == "__main__":
    asyncio.run(main())
