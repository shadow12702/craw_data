# -*- coding: utf-8 -*-
import asyncio
import json
import logging
import os
import re
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

try:
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore

try:
    from crawl4ai import AsyncWebCrawler, CacheMode, CrawlerRunConfig
except Exception:  # pragma: no cover
    from crawl4ai import AsyncWebCrawler  # type: ignore

    CacheMode = None  # type: ignore
    CrawlerRunConfig = None  # type: ignore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_START_URL = "https://moh.gov.vn/"
DEFAULT_BASE_DOMAIN = "moh.gov.vn"
DEFAULT_OUTPUT_ROOT = "data_craw"
DEFAULT_CONCURRENCY = 6
DEFAULT_PAGE_TIMEOUT_MS = 300_000
DEFAULT_CONNECT_ERROR_RETRY_LIMIT = 3

TABLE_DATA_BOYTE = "data_boyte"
TABLE_IMAGE_BOYTE = "image_boyte"
TABLE_VIDEO_BOYTE = "video_boyte"
TABLE_FILE_BOYTE = "file_boyte"

MEDIA_TYPE_IMAGE = "image"
MEDIA_TYPE_VIDEO = "video"
MEDIA_TYPE_FILE = "file"

EVENTS_FILENAME_PREFIX = "events_boyte_db"

MAIN_CONTENT_TAG = "main"
MAIN_CONTENT_ID: str | None = None
MAIN_CONTENT_CLASS_CONTAINS: str | None = None

SKIP_URL_SUBSTRINGS = ("logout", "login", "wp-admin", "/admin", "mailto:", "tel:")
FILE_EXTENSIONS = (
    ".pdf",
    ".doc",
    ".docx",
    ".xls",
    ".xlsx",
    ".ppt",
    ".pptx",
    ".zip",
    ".rar",
    ".7z",
)
STATIC_ASSET_EXTENSIONS = (
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".webp",
    ".svg",
    ".ico",
    ".mp4",
    ".webm",
    ".mp3",
    ".wav",
    ".css",
    ".js",
)
FILTER_BANNED_TAGS = {"nav", "header", "footer", "aside"}
FILTER_BANNED_CLASS_MARKERS = (
    "breadcrumb",
    "bread-crumb",
    "breadcrumbs",
    "crumb",
    "menu",
    "navbar",
    "nav",
    "header",
    "footer",
    "site-footer",
    "site-header",
    "topbar",
)
FILTER_CLASS_TAGS = {"div", "section", "ul", "ol", "nav", "header", "footer", "aside"}
TEXT_HEAD_START_MARKER = "trang chủ"
TEXT_TAIL_MARKERS = (
    "©",
    "all rights reserved",
    "điều khoản sử dụng",
    "chính sách bảo mật",
    "chính sách thanh toán",
    "chính sách đổi trả",
    "liên hệ",
    "hotline",
    "email",
)
CONTENT_FILTER_BANNED_CLASS_MARKERS = ("ft-chatbox-skin5", "slidebar", "post-sidebar")

TITLE_TEXT_LIMIT = 5000
MEDIA_TEXT_LIMIT = 2000
FILE_TEXT_LIMIT = 5000

# Windows PowerShell default encoding can be cp1252/cp936 and crash on unicode output.
try:  # pragma: no cover
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
except Exception:
    pass


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _safe_text(s: str, limit: int = 255) -> str:
    return (s or "").strip()[:limit]


def _short_hash(s: str) -> str:
    import hashlib

    return hashlib.sha1((s or "").encode("utf-8", errors="ignore")).hexdigest()[:10]


def _is_file_url(url: str) -> bool:
    u = (url or "").lower().split("?")[0].split("#")[0]
    return any(u.endswith(ext) for ext in FILE_EXTENSIONS)


def _is_crawlable_page_url(url: str) -> bool:
    u = (url or "").lower()
    if any(s in u for s in SKIP_URL_SUBSTRINGS):
        return False
    path = (urlparse(url).path or "").lower()
    if not path:
        return True
    if any(path.endswith(ext) for ext in STATIC_ASSET_EXTENSIONS):
        return False
    if _is_file_url(url):
        return False
    return True


_RETRYABLE_CONNECTION_ERROR_MARKERS = (
    "err_connection_reset",
    "err_connection_closed",
    "err_connection_timed_out",
    "err_connection_refused",
    "err_name_not_resolved",
    "err_network_changed",
    "timed out",
    "timeout",
    "connection reset",
    "connection refused",
    "connection aborted",
    "temporarily unavailable",
)


def _is_retryable_connection_error(reason: Optional[str]) -> bool:
    text = (reason or "").lower()
    if not text:
        return False
    if "crawl_failed" not in text and "fetch_error" not in text:
        return False
    return any(marker in text for marker in _RETRYABLE_CONNECTION_ERROR_MARKERS)


try:
    from scrapling_demo.crawl4ai_benhvien_scraper import (  # type: ignore
        CrawlState,
        MediaItem,
        _HTMLFilterRemoveDivByClass,
        _HTMLTextExtractor,
        canonicalize_video_url,
        clean_content_text,
        extract_media,
        extract_title_from_html,
        extract_title_from_markdown,
        is_related_domain,
        normalize_url,
        strip_navigation_menu_from_markdown,
    )
except Exception:
    from scrapling_demo.crawl4ai_benhvien_scraper import (  # type: ignore
        CrawlState,
        MediaItem,
        _HTMLFilterRemoveDivByClass,
        _HTMLTextExtractor,
        canonicalize_video_url,
        clean_content_text,
        extract_media,
        extract_title_from_html,
        extract_title_from_markdown,
        is_related_domain,
        normalize_url,
        strip_navigation_menu_from_markdown,
    )


def extract_main_content_html_any_site(html: str) -> str:
    """
    Generic default policy: only take inner HTML of the first <main>.
    Return "" if <main> is missing/empty so the caller can skip the page.
    """
    if not html:
        return ""

    from html.parser import HTMLParser

    class _Inner(HTMLParser):
        def __init__(
            self, *, tag: str, id_equals: str | None, class_contains: str | None
        ):
            super().__init__(convert_charrefs=False)
            self.tag = (tag or "").lower()
            self.id_equals = id_equals
            self.class_contains = (class_contains or "").lower().strip() or None
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

        def _is_target(self, tag: str, attrs) -> bool:
            if (tag or "").lower() != self.tag:
                return False
            ad = self._attrs_dict(attrs)
            if self.id_equals is not None and str(ad.get("id") or "") != self.id_equals:
                return False
            if self.class_contains:
                cls = str(ad.get("class") or "").lower()
                if self.class_contains not in cls:
                    return False
            return True

        def handle_starttag(self, tag, attrs):
            if not self._capturing:
                if self._is_target(tag, attrs):
                    self._capturing = True
                    self._depth = 1
                return
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

    p = _Inner(
        tag=MAIN_CONTENT_TAG,
        id_equals=MAIN_CONTENT_ID,
        class_contains=MAIN_CONTENT_CLASS_CONTAINS,
    )
    try:
        p.feed(html)
        p.close()
        out = p.html()
        return out if (out or "").strip() else ""
    except Exception:
        return ""


def extract_links_from_main_html(main_html: str, page_url: str) -> list[str]:
    h = main_html or ""
    if not h:
        return []
    out: list[str] = []
    for m in re.finditer(
        r"<a\b[^>]*href\s*=\s*['\"]([^'\"]+)['\"][^>]*>",
        h,
        flags=re.IGNORECASE,
    ):
        href = unescape((m.group(1) or "").strip())
        if not href:
            continue
        nu = normalize_url(href, page_url)
        if nu:
            out.append(nu)
    seen: set[str] = set()
    deduped: list[str] = []
    for u in out:
        if u not in seen:
            seen.add(u)
            deduped.append(u)
    return deduped


def filter_main_html_for_content(main_html: str) -> str:
    if not main_html:
        return ""

    from html.parser import HTMLParser

    class _Filter(HTMLParser):
        def __init__(self):
            super().__init__(convert_charrefs=False)
            self._skip_depth = 0
            self._chunks: list[str] = []

        @staticmethod
        def _attrs_dict(attrs) -> dict[str, str]:
            try:
                return {k.lower(): (v or "") for k, v in (attrs or []) if k}
            except Exception:
                return {}

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

        @staticmethod
        def _should_skip_by_class(tag: str, attrs) -> bool:
            t = (tag or "").lower()
            if t not in FILTER_CLASS_TAGS:
                return False
            ad = _Filter._attrs_dict(attrs)
            cls = (ad.get("class") or "").lower()
            if not cls:
                return False
            return any(m in cls for m in FILTER_BANNED_CLASS_MARKERS)

        def handle_starttag(self, tag, attrs):
            t = (tag or "").lower()
            if self._skip_depth > 0:
                self._skip_depth += 1
                return
            if t in FILTER_BANNED_TAGS or self._should_skip_by_class(tag, attrs):
                self._skip_depth = 1
                return
            self._chunks.append(self._rebuild_start(tag, attrs))

        def handle_startendtag(self, tag, attrs):
            if self._skip_depth > 0:
                return
            self._chunks.append(self._rebuild_start(tag, attrs)[:-1] + " />")

        def handle_endtag(self, tag):
            if self._skip_depth > 0:
                self._skip_depth -= 1
                return
            self._chunks.append(f"</{tag}>")

        def handle_data(self, data):
            if self._skip_depth <= 0 and data:
                self._chunks.append(data)

        def handle_entityref(self, name):
            if self._skip_depth <= 0:
                self._chunks.append(f"&{name};")

        def handle_charref(self, name):
            if self._skip_depth <= 0:
                self._chunks.append(f"&#{name};")

        def html(self) -> str:
            return "".join(self._chunks)

    p = _Filter()
    try:
        p.feed(main_html)
        p.close()
        return p.html()
    except Exception:
        return main_html


def strip_breadcrumb_and_footer_text(text: str) -> str:
    if not text:
        return ""
    lines = [ln.strip() for ln in (text or "").splitlines()]
    lines = [ln for ln in lines if ln != ""]
    if not lines:
        return ""

    head_cut = 0
    for i in range(min(12, len(lines))):
        if i == 0 and lines[i].lower().startswith(TEXT_HEAD_START_MARKER):
            head_cut = i + 1
            continue
        if head_cut > 0:
            if len(lines[i]) <= 60 and not re.search(r"[\.:\?]", lines[i]):
                head_cut = i + 1
                continue
            break
    if head_cut:
        lines = lines[head_cut:]

    tail_idx = None
    for i in range(len(lines) - 1, max(-1, len(lines) - 40), -1):
        l = lines[i].lower()
        if any(m in l for m in TEXT_TAIL_MARKERS):
            tail_idx = i
    if tail_idx is not None:
        lines = lines[:tail_idx]

    return "\n\n".join(lines).strip()


@dataclass(frozen=True)
class FileItem:
    url: str
    name: str


def extract_files(markdown: str, html: str, page_url: str) -> list[FileItem]:
    md = markdown or ""
    h = html or ""
    out: dict[str, str] = {}

    for m in re.finditer(r"\[([^\]]+)\]\(([^)]+)\)", md):
        text = (m.group(1) or "").strip()
        raw = (m.group(2) or "").strip().strip('"').strip("'")
        nu = normalize_url(raw, page_url) if raw else None
        if not nu or not _is_file_url(nu):
            continue
        out.setdefault(nu, text or Path(urlparse(nu).path or "").name or "file")

    for m in re.finditer(
        r"<a\b[^>]*href\s*=\s*['\"]([^'\"]+)['\"][^>]*>(.*?)</a>",
        h,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        href = unescape((m.group(1) or "").strip())
        body = re.sub(r"<[^>]+>", " ", m.group(2) or "")
        body = re.sub(r"\s+", " ", unescape(body)).strip()
        nu = normalize_url(href, page_url) if href else None
        if not nu or not _is_file_url(nu):
            continue
        out.setdefault(nu, body or Path(urlparse(nu).path or "").name or "file")

    return [FileItem(url=u, name=n or "file") for u, n in out.items()]


@dataclass
class PageItem:
    url: str
    title: str
    content: str
    images: list[MediaItem]
    videos: list[MediaItem]
    files: list[FileItem]
    crawl_time: datetime
    source_domain: str


class PostgresWriterBoYTe:
    def __init__(self):
        try:
            from scrapling_demo.db_config import (
                load_postgres_config,
                qualified_table_name,
            )
        except Exception:
            from scrapling_demo.db_config import load_postgres_config, qualified_table_name  # type: ignore

        self.cfg = load_postgres_config()
        self._qualified_table_name = qualified_table_name
        self._sem = asyncio.Semaphore(2)

    async def start(self) -> None:
        if psycopg is None:
            raise RuntimeError("Missing psycopg. Install: pip install psycopg[binary]")

    async def stop(self) -> None:
        return

    @staticmethod
    def _doc_id_from_url(url: str) -> uuid.UUID:
        return uuid.uuid5(uuid.NAMESPACE_URL, url)

    @staticmethod
    def _child_id(doc_id: uuid.UUID, kind: str, media_url: str) -> uuid.UUID:
        return uuid.uuid5(uuid.NAMESPACE_URL, f"{doc_id}|{kind}|{media_url}")

    async def write(self, page: PageItem) -> None:
        async with self._sem:
            await asyncio.to_thread(self._upsert_page, page)

    def _upsert_page(self, page: PageItem) -> None:
        schema = getattr(self.cfg, "schema", None) or "public"
        t_data = self._qualified_table_name(TABLE_DATA_BOYTE, schema)
        t_img = self._qualified_table_name(TABLE_IMAGE_BOYTE, schema)
        t_vid = self._qualified_table_name(TABLE_VIDEO_BOYTE, schema)
        t_file = self._qualified_table_name(TABLE_FILE_BOYTE, schema)

        doc_id = self._doc_id_from_url(page.url)

        with psycopg.connect(self.cfg.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {t_data} (id, title, content, link, source_domain, crawl_time)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE
                      SET title = EXCLUDED.title,
                          content = EXCLUDED.content,
                          link = EXCLUDED.link,
                          source_domain = EXCLUDED.source_domain,
                          crawl_time = EXCLUDED.crawl_time;
                    """,
                    (
                        doc_id,
                        _safe_text(page.title, TITLE_TEXT_LIMIT),
                        page.content,
                        page.url,
                        page.source_domain,
                        page.crawl_time,
                    ),
                )

                if page.images:
                    cur.executemany(
                        f"""
                        INSERT INTO {t_img} (id, data_id, media_type, image_url, content)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE
                          SET image_url = EXCLUDED.image_url,
                              content = EXCLUDED.content;
                        """,
                        [
                            (
                                self._child_id(doc_id, "image", it.url),
                                doc_id,
                                MEDIA_TYPE_IMAGE,
                                it.url,
                                (it.name or "none")[:MEDIA_TEXT_LIMIT],
                            )
                            for it in page.images
                            if (it.url or "").strip()
                        ],
                    )

                if page.videos:
                    vids = [
                        MediaItem(url=canonicalize_video_url(v.url), name=v.name)
                        for v in page.videos
                        if (v.url or "").strip()
                    ]
                    cur.executemany(
                        f"""
                        INSERT INTO {t_vid} (id, data_id, media_type, video_url, content)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE
                          SET video_url = EXCLUDED.video_url,
                              content = EXCLUDED.content;
                        """,
                        [
                            (
                                self._child_id(doc_id, "video", it.url),
                                doc_id,
                                MEDIA_TYPE_VIDEO,
                                it.url,
                                (it.name or "none")[:MEDIA_TEXT_LIMIT],
                            )
                            for it in vids
                        ],
                    )

                if page.files:
                    cur.executemany(
                        f"""
                        INSERT INTO {t_file} (id, data_id, media_type, file_name, file_url, content)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE
                          SET file_name = EXCLUDED.file_name,
                              file_url = EXCLUDED.file_url,
                              content = EXCLUDED.content;
                        """,
                        [
                            (
                                self._child_id(doc_id, "file", it.url),
                                doc_id,
                                MEDIA_TYPE_FILE,
                                _safe_text(it.name, FILE_TEXT_LIMIT),
                                it.url,
                                _safe_text(it.name, FILE_TEXT_LIMIT),
                            )
                            for it in page.files
                            if (it.url or "").strip()
                        ],
                    )
            conn.commit()


class CrawlBoYTeScraper:
    def __init__(
        self,
        start_url: str = DEFAULT_START_URL,
        *,
        max_pages: Optional[int] = None,
        concurrency: int = DEFAULT_CONCURRENCY,
        page_timeout_ms: int = DEFAULT_PAGE_TIMEOUT_MS,
        output_root: str = DEFAULT_OUTPUT_ROOT,
        resume: bool = True,
        retry_failed_on_resume: bool = False,
        allow_domains: Optional[list[str]] = None,
    ):
        self.start_url = start_url
        host = urlparse(start_url).netloc.lower().strip(".")
        if host.startswith("www."):
            host = host[4:]
        self.base_domain = host or DEFAULT_BASE_DOMAIN
        self.source_domain = self.base_domain

        self.max_pages = max_pages if max_pages and int(max_pages) > 0 else None
        self.concurrency = max(1, int(concurrency))
        self.page_timeout_ms = int(page_timeout_ms)
        self.output_root = output_root
        self.resume = bool(resume)
        self.retry_failed_on_resume = bool(retry_failed_on_resume)
        self.allow_domains = allow_domains or []

        self.crawler: Optional[AsyncWebCrawler] = None
        self.db_writer = PostgresWriterBoYTe()
        domain_tag = re.sub(
            r"[^a-z0-9]+", "_", (self.base_domain or "site").lower()
        ).strip("_")
        self.state = CrawlState(
            output_root=self.output_root,
            events_filename=f"{EVENTS_FILENAME_PREFIX}_{domain_tag}_{_short_hash(self.base_domain)}.jsonl",
        )

        self.visited_urls: set[str] = set()
        self.failed_urls: set[str] = set()
        self.processing_urls: set[str] = set()
        self.queued_urls: set[str] = set()
        self.connect_error_retry_limit = DEFAULT_CONNECT_ERROR_RETRY_LIMIT

    def _is_allowed(self, url: str) -> bool:
        return is_related_domain(url, self.base_domain, self.allow_domains)

    async def _crawl_one(
        self, url: str
    ) -> tuple[Optional[PageItem], list[str], Optional[str]]:
        if url in self.visited_urls:
            return None, [], None
        if self.max_pages and len(self.visited_urls) >= self.max_pages:
            return None, [], "max_pages_reached"
        if not self._is_allowed(url) or not _is_crawlable_page_url(url):
            return None, [], "not_allowed_or_skipped"
        if url in self.processing_urls:
            return None, [], "already_processing"

        self.processing_urls.add(url)
        logger.info(
            f"[HTML] Crawling: {url} ({len(self.visited_urls)}/{self.max_pages or '∞'})"
        )

        try:
            if CrawlerRunConfig is not None:
                kwargs = {"page_timeout": self.page_timeout_ms}
                if CacheMode is not None:
                    kwargs["cache_mode"] = CacheMode.BYPASS
                run_config = CrawlerRunConfig(**kwargs)
                result = await self.crawler.arun(url=url, config=run_config)  # type: ignore[union-attr]
            else:
                result = await self.crawler.arun(url=url)  # type: ignore[union-attr]

            if not getattr(result, "success", False):
                msg = getattr(result, "error_message", "") or "unknown error"
                return None, [], f"crawl_failed: {msg}"

            markdown = getattr(result, "markdown", "") or ""
            html = getattr(result, "html", "") or ""

            title = (
                extract_title_from_markdown(markdown)
                or extract_title_from_html(html)
                or url
            )

            entry_html = extract_main_content_html_any_site(html)
            if not (entry_html or "").strip():
                return None, [], "no_main"

            entry_html = filter_main_html_for_content(entry_html)

            discovered_main = extract_links_from_main_html(entry_html, url)
            deduped: list[str] = []
            for nu in discovered_main:
                if not nu or not self._is_allowed(nu):
                    continue
                if _is_crawlable_page_url(nu):
                    deduped.append(nu)

            md_for_media = (
                strip_navigation_menu_from_markdown(markdown) if markdown else ""
            )
            images, videos = extract_media(md_for_media, entry_html, url)
            files = extract_files(md_for_media, entry_html, url)

            html_filter = _HTMLFilterRemoveDivByClass(
                banned_class_markers=CONTENT_FILTER_BANNED_CLASS_MARKERS
            )
            try:
                html_filter.feed(entry_html)
                html_filter.close()
                cleaned_entry_html = html_filter.html()
            except Exception:
                cleaned_entry_html = entry_html

            t = _HTMLTextExtractor()
            try:
                t.feed(cleaned_entry_html)
                t.close()
                content_md = t.text()
            except Exception:
                content_md = ""

            content_md = clean_content_text(content_md)
            content_md = strip_breadcrumb_and_footer_text(content_md)

            item = PageItem(
                url=url,
                title=title,
                content=content_md,
                images=images,
                videos=videos,
                files=files,
                crawl_time=_now_utc(),
                source_domain=self.source_domain,
            )
            return item, deduped, None
        except Exception as e:
            return None, [], f"fetch_error: {e}"
        finally:
            self.processing_urls.discard(url)

    async def _crawl_one_with_retry(
        self, url: str
    ) -> tuple[Optional[PageItem], list[str], Optional[str]]:
        last_result: tuple[Optional[PageItem], list[str], Optional[str]] = (None, [], None)
        max_attempts = 1 + int(self.connect_error_retry_limit)
        for attempt in range(1, max_attempts + 1):
            page, new_urls, skip_reason = await self._crawl_one(url)
            last_result = (page, new_urls, skip_reason)
            if page is not None:
                return last_result
            if not _is_retryable_connection_error(skip_reason):
                return last_result
            if attempt >= max_attempts:
                break
            wait_s = min(attempt, 3)
            await self.state.record(
                "retry",
                url,
                ok=False,
                error=f"attempt={attempt}/{self.connect_error_retry_limit}; wait_s={wait_s}; reason={skip_reason}",
            )
            logger.warning(
                f"[HTML] Retry {attempt}/{self.connect_error_retry_limit} after connection error: {url} | {skip_reason}"
            )
            await asyncio.sleep(wait_s)
        return last_result

    async def _worker(self, q: "asyncio.Queue[str]") -> None:
        while True:
            url = await q.get()
            try:
                if url in self.visited_urls:
                    continue
                if (not self.retry_failed_on_resume) and (url in self.failed_urls):
                    continue

                await self.state.record("start", url)
                page, new_urls, skip_reason = await self._crawl_one_with_retry(url)
                if page is not None:
                    await self.db_writer.write(page)
                    self.visited_urls.add(url)
                    await self.state.record("done", url, ok=True)
                else:
                    if skip_reason and skip_reason.startswith("max_pages_reached"):
                        await self.state.record(
                            "done", url, ok=False, error=skip_reason
                        )
                        self.failed_urls.add(url)
                        logger.warning(f"[HTML] Stop (max pages): {url}")
                    elif skip_reason and skip_reason.startswith("crawl_failed"):
                        await self.state.record(
                            "done", url, ok=False, error=skip_reason
                        )
                        self.failed_urls.add(url)
                        logger.error(f"[HTML] Crawl failed: {url} | {skip_reason}")
                    elif skip_reason and skip_reason.startswith("fetch_error"):
                        await self.state.record(
                            "done", url, ok=False, error=skip_reason
                        )
                        self.failed_urls.add(url)
                        logger.error(f"[HTML] Fetch error: {url} | {skip_reason}")
                    else:
                        await self.state.record(
                            "done", url, ok=True, error=skip_reason or ""
                        )
                        self.visited_urls.add(url)
                        if skip_reason:
                            logger.info(f"[HTML] Skip: {url} | {skip_reason}")

                for nu in new_urls:
                    if nu in self.visited_urls:
                        continue
                    if (not self.retry_failed_on_resume) and (nu in self.failed_urls):
                        continue
                    if nu in self.queued_urls:
                        continue
                    self.queued_urls.add(nu)
                    await self.state.record("enqueue", nu)
                    q.put_nowait(nu)
            finally:
                q.task_done()

    async def run(self) -> None:
        await self.db_writer.start()
        q: asyncio.Queue[str] = asyncio.Queue()

        if self.resume:
            enqueued, started, done_ok, done_failed = self.state.load()
            self.visited_urls = set(done_ok)
            self.failed_urls = (
                set() if self.retry_failed_on_resume else set(done_failed)
            )
            pending = (
                (set(enqueued) | set(started))
                - set(done_ok)
                - (set() if self.retry_failed_on_resume else set(done_failed))
            )
            pending.add(self.start_url)
            for u in sorted(pending):
                if u in self.visited_urls:
                    continue
                if (not self.retry_failed_on_resume) and (u in self.failed_urls):
                    continue
                if u in self.queued_urls:
                    continue
                self.queued_urls.add(u)
                q.put_nowait(u)
        else:
            self.queued_urls.add(self.start_url)
            await self.state.record("enqueue", self.start_url)
            q.put_nowait(self.start_url)

        async with AsyncWebCrawler() as crawler:
            self.crawler = crawler
            workers = [
                asyncio.create_task(self._worker(q)) for _ in range(self.concurrency)
            ]
            await q.join()
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            self.crawler = None

        await self.db_writer.stop()


def _parse_args(argv: list[str]) -> dict:
    import argparse

    p = argparse.ArgumentParser(
        description=f"Crawl {DEFAULT_BASE_DOMAIN} and store into *_boyte tables (with resume)."
    )
    p.add_argument("--start-url", default=DEFAULT_START_URL)
    p.add_argument("--max-pages", type=int, default=0, help="0 = unlimited")
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    p.add_argument("--page-timeout-ms", type=int, default=DEFAULT_PAGE_TIMEOUT_MS)
    p.add_argument("--output-root", default=DEFAULT_OUTPUT_ROOT)
    p.add_argument("--no-resume", action="store_true")
    p.add_argument("--retry-failed-on-resume", action="store_true")
    p.add_argument(
        "--allow-domain",
        action="append",
        default=[],
        help="Additional allowed domains (repeatable).",
    )
    return vars(p.parse_args(argv))


async def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    scraper = CrawlBoYTeScraper(
        start_url=args["start_url"],
        max_pages=(
            (args["max_pages"] or None) if int(args["max_pages"] or 0) > 0 else None
        ),
        concurrency=args["concurrency"],
        page_timeout_ms=args["page_timeout_ms"],
        output_root=args["output_root"],
        resume=not bool(args["no_resume"]),
        retry_failed_on_resume=bool(args["retry_failed_on_resume"]),
        allow_domains=args["allow_domain"] or [],
    )

    Path(args["output_root"]).mkdir(parents=True, exist_ok=True)
    await scraper.run()
    logger.info(
        f"Done. Exported to Postgres tables: {TABLE_DATA_BOYTE}, {TABLE_IMAGE_BOYTE}, {TABLE_VIDEO_BOYTE}, {TABLE_FILE_BOYTE}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
