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

# =========================
# Config (easy to customize)
# =========================

DEFAULT_START_URL = "https://thuocbietduoc.com.vn/"
DEFAULT_OUTPUT_ROOT = "data_craw"

DEFAULT_MAX_PAGES = 0  # 0 = unlimited
DEFAULT_CONCURRENCY = 6
DEFAULT_BROWSER_CONCURRENCY = 2
DEFAULT_PAGE_TIMEOUT_MS = 300_000
DEFAULT_RECYCLE_EVERY = 400  # 0 disables

DEFAULT_CONNECT_ERROR_RETRY_LIMIT = 3

DEFAULT_HTML_FILTER_BANNED_CLASS_MARKERS = (
    "ft-chatbox-skin5",
    "slidebar",
    "post-sidebar",
)

DEFAULT_MAIN_BANNED_TAGS = {"nav", "header", "footer", "aside"}
DEFAULT_MAIN_BANNED_CLASS_MARKERS = (
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

DEFAULT_FOOTER_TAIL_MARKERS = (
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

# URL sanity guard (avoid malformed/template URLs in the queue)
DEFAULT_SKIP_URL_CONTAINS = ("${", "}")
DEFAULT_SKIP_URL_IF_HAS_WHITESPACE = True

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
    return any(
        u.endswith(ext)
        for ext in (
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
    )


def _is_crawlable_page_url(url: str) -> bool:
    u = (url or "").lower()
    # Guard against template/invalid URLs that can slip from malformed HTML.
    if any(tok in u for tok in DEFAULT_SKIP_URL_CONTAINS):
        return False
    if DEFAULT_SKIP_URL_IF_HAS_WHITESPACE and any(ch.isspace() for ch in u):
        return False
    if any(
        s in u for s in ("logout", "login", "wp-admin", "/admin", "mailto:", "tel:")
    ):
        return False
    path = (urlparse(url).path or "").lower()
    if not path:
        return True
    # Don't crawl static/media/files; we only store them as attachments.
    if any(
        path.endswith(ext)
        for ext in (
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
    ):
        return False
    if _is_file_url(url):
        return False
    return True


DEFAULT_RETRYABLE_CONNECTION_ERROR_MARKERS = (
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
    # Playwright resource pressure / remote object GC
    "unbounded heap growth",
    "object has been collected",
)


def _is_retryable_connection_error(reason: Optional[str]) -> bool:
    text = (reason or "").lower()
    if not text:
        return False
    if "crawl_failed" not in text and "fetch_error" not in text:
        return False
    return any(marker in text for marker in DEFAULT_RETRYABLE_CONNECTION_ERROR_MARKERS)


# Reuse proven extraction/normalization utilities from the existing scraper.
try:
    from scrapling_demo.crawl4ai_benhvien_scraper import (  # type: ignore
        CrawlState,
        MediaItem,
        _HTMLFilterRemoveDivByClass,
        _HTMLTextExtractor,
        _coerce_link,
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
        _coerce_link,
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
    STRICT policy (per user request): only take inner HTML of the first <main>.
    Return "" if <main> is missing/empty (caller should skip the page).
    """
    if not html:
        return ""

    # Minimal inner HTML extractor (first match only).
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

    p = _Inner(tag="main", id_equals=None, class_contains=None)
    try:
        p.feed(html)
        p.close()
        out = p.html()
        return out if (out or "").strip() else ""
    except Exception:
        return ""
    return ""


def extract_links_from_main_html(main_html: str, page_url: str) -> list[str]:
    """
    Discover links ONLY from <main> (avoid <header>/<footer> menus).
    Returns normalized absolute URLs (may include file URLs; caller decides what to do).
    """
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
    # de-dupe preserving order
    seen: set[str] = set()
    deduped: list[str] = []
    for u in out:
        if u not in seen:
            seen.add(u)
            deduped.append(u)
    return deduped


def filter_main_html_for_content(main_html: str) -> str:
    """
    Remove navigation/footer-like blocks that can still exist inside <main>.
    This is intentionally heuristic for thuocbietduoc.com.vn:
    - drop <nav>, <header>, <footer>, <aside>
    - drop containers with breadcrumb/menu/navbar/footer-like classes
    """
    if not main_html:
        return ""

    from html.parser import HTMLParser

    banned_tags = DEFAULT_MAIN_BANNED_TAGS
    banned_class_markers = DEFAULT_MAIN_BANNED_CLASS_MARKERS

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
            # only consider skipping common container tags
            if t not in {
                "div",
                "section",
                "ul",
                "ol",
                "nav",
                "header",
                "footer",
                "aside",
            }:
                return False
            ad = _Filter._attrs_dict(attrs)
            cls = (ad.get("class") or "").lower()
            if not cls:
                return False
            return any(m in cls for m in banned_class_markers)

        def handle_starttag(self, tag, attrs):
            t = (tag or "").lower()
            if self._skip_depth > 0:
                self._skip_depth += 1
                return
            if t in banned_tags or self._should_skip_by_class(tag, attrs):
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
    """
    Post-process extracted text to remove breadcrumb-like head and footer-like tail
    that may still slip through.
    """
    if not text:
        return ""
    lines = [ln.strip() for ln in (text or "").splitlines()]
    lines = [ln for ln in lines if ln != ""]
    if not lines:
        return ""

    # Drop breadcrumb head (usually starts with "Trang chủ")
    head_cut = 0
    for i in range(min(12, len(lines))):
        if i == 0 and lines[i].lower().startswith("trang chủ"):
            head_cut = i + 1
            continue
        if head_cut > 0:
            # keep cutting short category-like crumbs
            if len(lines[i]) <= 60 and not re.search(r"[\.:\?]", lines[i]):
                head_cut = i + 1
                continue
            break
    if head_cut:
        lines = lines[head_cut:]

    # Drop footer tail (policies/contact/copyright)
    tail_markers = DEFAULT_FOOTER_TAIL_MARKERS
    tail_idx = None
    for i in range(len(lines) - 1, max(-1, len(lines) - 40), -1):
        l = lines[i].lower()
        if any(m in l for m in tail_markers):
            tail_idx = i
    if tail_idx is not None:
        lines = lines[:tail_idx]

    return "\n\n".join(lines).strip()


@dataclass(frozen=True)
class FileItem:
    url: str
    name: str  # anchor text / title / filename


def extract_files(markdown: str, html: str, page_url: str) -> list[FileItem]:
    md = markdown or ""
    h = html or ""
    out: dict[str, str] = {}

    # Markdown links: [text](url)
    for m in re.finditer(r"\[([^\]]+)\]\(([^)]+)\)", md):
        text = (m.group(1) or "").strip()
        raw = (m.group(2) or "").strip().strip('"').strip("'")
        nu = normalize_url(raw, page_url) if raw else None
        if not nu or not _is_file_url(nu):
            continue
        out.setdefault(nu, text or Path(urlparse(nu).path or "").name or "file")

    # HTML anchors
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


class PostgresWriterBietDuoc:
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
        t_data = self._qualified_table_name("data_bietduoc", schema)
        t_img = self._qualified_table_name("image_bietduoc", schema)
        t_vid = self._qualified_table_name("video_bietduoc", schema)
        t_file = self._qualified_table_name("file_bietduoc", schema)

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
                        _safe_text(page.title, 5000),
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
                        VALUES (%s, %s, 'image', %s, %s)
                        ON CONFLICT (id) DO UPDATE
                          SET image_url = EXCLUDED.image_url,
                              content = EXCLUDED.content;
                        """,
                        [
                            (
                                self._child_id(doc_id, "image", it.url),
                                doc_id,
                                it.url,
                                (it.name or "none")[:2000],
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
                        VALUES (%s, %s, 'video', %s, %s)
                        ON CONFLICT (id) DO UPDATE
                          SET video_url = EXCLUDED.video_url,
                              content = EXCLUDED.content;
                        """,
                        [
                            (
                                self._child_id(doc_id, "video", it.url),
                                doc_id,
                                it.url,
                                (it.name or "none")[:2000],
                            )
                            for it in vids
                        ],
                    )

                if page.files:
                    cur.executemany(
                        f"""
                        INSERT INTO {t_file} (id, data_id, media_type, file_name, file_url, content)
                        VALUES (%s, %s, 'file', %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE
                          SET file_name = EXCLUDED.file_name,
                              file_url = EXCLUDED.file_url,
                              content = EXCLUDED.content;
                        """,
                        [
                            (
                                self._child_id(doc_id, "file", it.url),
                                doc_id,
                                _safe_text(it.name, 5000),
                                it.url,
                                _safe_text(it.name, 5000),
                            )
                            for it in page.files
                            if (it.url or "").strip()
                        ],
                    )
            conn.commit()


class CrawlThuocBietDuocScraper:
    def __init__(
        self,
        start_url: str = DEFAULT_START_URL,
        *,
        max_pages: Optional[int] = None,
        concurrency: int = DEFAULT_CONCURRENCY,
        browser_concurrency: int = DEFAULT_BROWSER_CONCURRENCY,
        recycle_every: int = DEFAULT_RECYCLE_EVERY,
        page_timeout_ms: int = DEFAULT_PAGE_TIMEOUT_MS,
        output_root: str = DEFAULT_OUTPUT_ROOT,
        resume: bool = True,
        retry_failed_on_resume: bool = False,
        allow_domains: Optional[list[str]] = None,
        connect_error_retry_limit: int = DEFAULT_CONNECT_ERROR_RETRY_LIMIT,
    ):
        self.start_url = start_url
        host = urlparse(start_url).netloc.lower().strip(".")
        if host.startswith("www."):
            host = host[4:]
        self.base_domain = host or "thuocbietduoc.com.vn"
        self.source_domain = self.base_domain

        self.max_pages = max_pages if max_pages and int(max_pages) > 0 else None
        self.concurrency = max(1, int(concurrency))
        # Limit the number of concurrent Playwright page/context operations.
        # This reduces "The object has been collected to prevent unbounded heap growth".
        self.browser_concurrency = max(1, int(browser_concurrency))
        self._browser_sem = asyncio.Semaphore(self.browser_concurrency)
        # Periodically recycle crawler/browser to avoid long-run Playwright heap pressure.
        self.recycle_every = max(0, int(recycle_every or 0))
        self._arun_calls = 0
        self._arun_calls_lock = asyncio.Lock()
        self._recycle_lock = asyncio.Lock()
        self._crawler_ref_lock = asyncio.Lock()
        self._crawler_ctx: Optional[AsyncWebCrawler] = None
        self.page_timeout_ms = int(page_timeout_ms)
        self.output_root = output_root
        self.resume = bool(resume)
        self.retry_failed_on_resume = bool(retry_failed_on_resume)
        self.allow_domains = allow_domains or []

        self.crawler: Optional[AsyncWebCrawler] = None
        self.db_writer = PostgresWriterBietDuoc()
        domain_tag = re.sub(
            r"[^a-z0-9]+", "_", (self.base_domain or "site").lower()
        ).strip("_")
        self.state = CrawlState(
            output_root=self.output_root,
            events_filename=f"events_bietduoc_db_{domain_tag}_{_short_hash(self.base_domain)}.jsonl",
        )

        self.visited_urls: set[str] = set()
        self.failed_urls: set[str] = set()
        self.processing_urls: set[str] = set()
        self.queued_urls: set[str] = set()
        self.connect_error_retry_limit = max(0, int(connect_error_retry_limit))

    async def _start_crawler(self) -> None:
        ctx = AsyncWebCrawler()
        crawler = await ctx.__aenter__()
        async with self._crawler_ref_lock:
            self._crawler_ctx = ctx
            self.crawler = crawler

    async def _stop_crawler(self) -> None:
        async with self._crawler_ref_lock:
            ctx = self._crawler_ctx
            self._crawler_ctx = None
            self.crawler = None
        if ctx is not None:
            await ctx.__aexit__(None, None, None)

    async def _recycle_crawler(self, reason: str) -> None:
        if self.recycle_every <= 0:
            return
        async with self._recycle_lock:
            # Drain permits so no Playwright work is in-flight.
            for _ in range(self.browser_concurrency):
                await self._browser_sem.acquire()
            try:
                try:
                    await self._stop_crawler()
                except Exception as e:
                    logger.warning(f"[CRAWLER] Stop failed during recycle: {e}")
                await self._start_crawler()
                logger.info(
                    f"[CRAWLER] Recycled crawler (reason={reason}, arun_calls={self._arun_calls})"
                )
            finally:
                for _ in range(self.browser_concurrency):
                    self._browser_sem.release()

    async def _note_arun_call_and_maybe_recycle(self) -> None:
        if self.recycle_every <= 0:
            return
        do_recycle = False
        async with self._arun_calls_lock:
            self._arun_calls += 1
            if self.recycle_every > 0 and (self._arun_calls % self.recycle_every == 0):
                do_recycle = True
        if do_recycle:
            await self._recycle_crawler(reason=f"every_{self.recycle_every}")

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
            await self._browser_sem.acquire()
            try:
                async with self._crawler_ref_lock:
                    crawler = self.crawler
                if crawler is None:
                    return None, [], "crawl_failed: crawler_not_initialized"
                if CrawlerRunConfig is not None:
                    kwargs = {"page_timeout": self.page_timeout_ms}
                    if CacheMode is not None:
                        kwargs["cache_mode"] = CacheMode.BYPASS
                    run_config = CrawlerRunConfig(**kwargs)
                    result = await crawler.arun(url=url, config=run_config)
                else:
                    result = await crawler.arun(url=url)
            finally:
                self._browser_sem.release()
                await self._note_arun_call_and_maybe_recycle()

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

            # Discover links ONLY from <main> to avoid header/footer menus.
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
                banned_class_markers=DEFAULT_HTML_FILTER_BANNED_CLASS_MARKERS
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
        last_result: tuple[Optional[PageItem], list[str], Optional[str]] = (
            None,
            [],
            None,
        )
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
                        # skipped (not allowed / no content / already processing)
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
            # Ensure start_url is included
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

        workers: list[asyncio.Task] = []
        try:
            await self._start_crawler()
            workers = [
                asyncio.create_task(self._worker(q)) for _ in range(self.concurrency)
            ]
            await q.join()
        finally:
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            try:
                await self._stop_crawler()
            except Exception as e:
                logger.warning(f"[CRAWLER] Stop failed: {e}")
            await self.db_writer.stop()


def _parse_args(argv: list[str]) -> dict:
    import argparse

    p = argparse.ArgumentParser(
        description="Crawl thuocbietduoc.com.vn and store into *_bietduoc tables (with resume)."
    )
    p.add_argument("--start-url", default=DEFAULT_START_URL)
    p.add_argument(
        "--max-pages", type=int, default=DEFAULT_MAX_PAGES, help="0 = unlimited"
    )
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    p.add_argument(
        "--browser-concurrency",
        type=int,
        default=DEFAULT_BROWSER_CONCURRENCY,
        help="Limit concurrent Playwright page/context work (reduces 'object has been collected' errors).",
    )
    p.add_argument(
        "--recycle-every",
        type=int,
        default=DEFAULT_RECYCLE_EVERY,
        help="Recycle crawler/browser every N arun calls (0 disables). Helps long runs stability.",
    )
    p.add_argument("--page-timeout-ms", type=int, default=DEFAULT_PAGE_TIMEOUT_MS)
    p.add_argument("--output-root", default=DEFAULT_OUTPUT_ROOT)
    p.add_argument(
        "--connect-error-retry-limit",
        type=int,
        default=DEFAULT_CONNECT_ERROR_RETRY_LIMIT,
        help="Retries for retryable connection/Playwright transient errors.",
    )
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
    scraper = CrawlThuocBietDuocScraper(
        start_url=args["start_url"],
        max_pages=(
            (args["max_pages"] or None) if int(args["max_pages"] or 0) > 0 else None
        ),
        concurrency=args["concurrency"],
        browser_concurrency=args["browser_concurrency"],
        recycle_every=args["recycle_every"],
        page_timeout_ms=args["page_timeout_ms"],
        output_root=args["output_root"],
        resume=not bool(args["no_resume"]),
        retry_failed_on_resume=bool(args["retry_failed_on_resume"]),
        allow_domains=args["allow_domain"] or [],
        connect_error_retry_limit=args["connect_error_retry_limit"],
    )

    Path(args["output_root"]).mkdir(parents=True, exist_ok=True)
    await scraper.run()
    logger.info(
        "Done. Exported to Postgres tables: data_bietduoc, image_bietduoc, video_bietduoc, file_bietduoc"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
