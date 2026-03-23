# -*- coding: utf-8 -*-
"""
Crawl medinet.hochiminhcity.gov.vn (Sở Y tế TP.HCM — Medinet).

Nhiệm vụ tổng quát:
- Giống luồng `craw_thuocbietduoc.py`: BFS từ URL gốc, lọc menu/breadcrumb, trích media & file,
  hàng đợi đa worker, retry lỗi mạng tạm thời.
- Khác Medinet: nội dung trang lấy từ khối HTML có `id="MainContent"` (không dùng thẻ <main>).
- Chỉ ghi local: CSV trong thư mục `data/` (không dùng PostgreSQL).
- Resume: file JSONL append-only dưới `<output_root>/state/` (enqueue/start/done).
- Log: console + file `data/logs/soyte_crawl.log` để đối chiếu khi cần.

Chạy:
  python craw_soyte.py
  python craw_soyte.py --retry-failed-on-resume
  python craw_soyte.py --no-resume
"""

from __future__ import annotations

import asyncio
import csv
import logging
import re
import sys
from pathlib import Path
from typing import Optional, Tuple

# --- Crawler gốc: tái dùng toàn bộ logic crawl / queue / state machine ---
from craw_thuocbietduoc import (
    DEFAULT_BROWSER_CONCURRENCY,
    DEFAULT_CONNECT_ERROR_RETRY_LIMIT,
    DEFAULT_CONCURRENCY,
    DEFAULT_HTML_FILTER_BANNED_CLASS_MARKERS,
    DEFAULT_MAX_PAGES,
    DEFAULT_PAGE_TIMEOUT_MS,
    DEFAULT_RECYCLE_EVERY,
    PageItem,
    CrawlThuocBietDuocScraper,
    CacheMode,
    CrawlerRunConfig,
    _HTMLFilterRemoveDivByClass,
    _HTMLTextExtractor,
    _is_crawlable_page_url,
    _now_utc,
    clean_content_text,
    extract_files,
    extract_links_from_main_html,
    extract_media,
    extract_title_from_html,
    extract_title_from_markdown,
    filter_main_html_for_content,
    strip_breadcrumb_and_footer_text,
    strip_navigation_menu_from_markdown,
)
from scrapling_demo.crawl4ai_benhvien_scraper import CrawlState

logger = logging.getLogger(__name__)

# =========================
# Cấu hình mặc định (site Sở Y tế TP.HCM)
# =========================

# URL khởi đầu — điểm vào crawl (BFS).
DEFAULT_START_URL = "https://medinet.hochiminhcity.gov.vn/"

# Thư mục gốc: chứa CSV + (mặc định) state resume + logs con.
DEFAULT_OUTPUT_ROOT = "data"

# Tên file CSV (trong DEFAULT_OUTPUT_ROOT).
CSV_PAGES = "soyte_pages.csv"
CSV_IMAGES = "soyte_images.csv"
CSV_VIDEOS = "soyte_videos.csv"
CSV_FILES = "soyte_files.csv"

# File log để kiểm tra lỗi / tiến độ sau khi chạy nền.
LOG_REL_PATH = "logs/soyte_crawl.log"

# Medinet: vùng nội dung chính trong DOM (ASP.NET thường dùng div#MainContent).
DEFAULT_CONTENT_ELEMENT_ID = "MainContent"


def extract_inner_html_by_element_id(html: str, element_id: str = "MainContent") -> str:
    """
    Lấy inner HTML của phần tử đầu tiên có attribute id khớp (so sánh không phân biệt hoa thường).
    Dùng cho site đặt nội dung trong #MainContent thay vì <main>.
    """
    if not html:
        return ""
    target = (element_id or "").strip().lower()
    if not target:
        return ""

    from html.parser import HTMLParser

    class _InnerById(HTMLParser):
        def __init__(self) -> None:
            super().__init__(convert_charrefs=False)
            self._target = target
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

        def _match(self, attrs) -> bool:
            ad = self._attrs_dict(attrs)
            return str(ad.get("id") or "").strip().lower() == self._target

        def handle_starttag(self, tag, attrs):
            if not self._capturing:
                if self._match(attrs):
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

    p = _InnerById()
    try:
        p.feed(html)
        p.close()
        out = p.html()
        return out if (out or "").strip() else ""
    except Exception:
        return ""


def _short_hash(s: str) -> str:
    import hashlib

    return hashlib.sha1((s or "").encode("utf-8", errors="ignore")).hexdigest()[:10]


def _setup_logging(output_root: str) -> None:
    """Ghi log ra console (INFO) và file UTF-8 để đọc lại / grep."""
    root = Path(output_root)
    log_dir = root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / Path(LOG_REL_PATH).name

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    marker = "craw_soyte_file"
    if not any(getattr(h, "_craw_soyte", None) == marker for h in root_logger.handlers):
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh._craw_soyte = marker  # type: ignore[attr-defined]
        fh.setLevel(logging.INFO)
        fh.setFormatter(fmt)
        root_logger.addHandler(fh)
    # Console: dùng handler từ `craw_thuocbietduoc` (basicConfig); không thêm stderr thứ hai.


class CsvWriterSoyte:
    """
    Ghi bản ghi trang + media xuống CSV local (không ghi DB).

    - soyte_pages.csv: một dòng / URL (title, content, meta).
    - soyte_images.csv / soyte_videos.csv / soyte_files.csv: quan hệ 1-n với page_url.
    - Ghi append có khóa asyncio để tránh xen dòng khi nhiều worker.
    - utf-8-sig: mở tốt trên Excel Windows.
    """

    def __init__(self, data_root: Path, *, content_root_id: str = DEFAULT_CONTENT_ELEMENT_ID):
        self.data_root = Path(data_root)
        self.data_root.mkdir(parents=True, exist_ok=True)
        self._sem = asyncio.Semaphore(1)
        # Ghi nhận vùng DOM đã trích (cột CSV) — ví dụ MainContent.
        self._content_root_id = content_root_id or DEFAULT_CONTENT_ELEMENT_ID

    async def start(self) -> None:
        return

    async def stop(self) -> None:
        return

    def _append_csv(
        self,
        filename: str,
        fieldnames: list[str],
        row: dict[str, str],
    ) -> None:
        path = self.data_root / filename
        is_new = not path.exists() or path.stat().st_size == 0
        with open(path, "a", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(
                f,
                fieldnames=fieldnames,
                quoting=csv.QUOTE_MINIMAL,
                extrasaction="ignore",
            )
            if is_new:
                w.writeheader()
            w.writerow(row)

    def _write_page_sync(self, page: PageItem) -> None:
        crawl_ts = page.crawl_time.isoformat()
        self._append_csv(
            CSV_PAGES,
            [
                "url",
                "title",
                "content",
                "source_domain",
                "crawl_time_utc",
                "content_root_id",
            ],
            {
                "url": page.url,
                "title": page.title,
                "content": page.content or "",
                "source_domain": page.source_domain,
                "crawl_time_utc": crawl_ts,
                "content_root_id": self._content_root_id,
            },
        )
        for it in page.images:
            if not (it.url or "").strip():
                continue
            self._append_csv(
                CSV_IMAGES,
                ["page_url", "image_url", "label"],
                {
                    "page_url": page.url,
                    "image_url": it.url,
                    "label": (it.name or "")[:2000],
                },
            )
        for it in page.videos:
            if not (it.url or "").strip():
                continue
            self._append_csv(
                CSV_VIDEOS,
                ["page_url", "video_url", "label"],
                {
                    "page_url": page.url,
                    "video_url": it.url,
                    "label": (it.name or "")[:2000],
                },
            )
        for it in page.files:
            if not (it.url or "").strip():
                continue
            self._append_csv(
                CSV_FILES,
                ["page_url", "file_name", "file_url"],
                {
                    "page_url": page.url,
                    "file_name": it.name or "",
                    "file_url": it.url,
                },
            )

    async def write(self, page: PageItem) -> None:
        async with self._sem:
            await asyncio.to_thread(self._write_page_sync, page)


class CrawlSoyteScraper(CrawlThuocBietDuocScraper):
    """
    Crawler Medinet: cùng queue / workers / resume với thuocbietduoc;
    `_crawl_one` lấy HTML từ `#MainContent` (id DOM); chỉ ghi CSV local qua `CsvWriterSoyte`.
    """

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
        content_element_id: str = DEFAULT_CONTENT_ELEMENT_ID,
    ):
        super().__init__(
            start_url=start_url,
            max_pages=max_pages,
            concurrency=concurrency,
            browser_concurrency=browser_concurrency,
            recycle_every=recycle_every,
            page_timeout_ms=page_timeout_ms,
            output_root=output_root,
            resume=resume,
            retry_failed_on_resume=retry_failed_on_resume,
            allow_domains=allow_domains,
            connect_error_retry_limit=connect_error_retry_limit,
        )
        self.content_element_id = (content_element_id or DEFAULT_CONTENT_ELEMENT_ID).strip()
        self.db_writer = CsvWriterSoyte(
            Path(self.output_root), content_root_id=self.content_element_id
        )
        # State resume: tách tên file với crawl thuocbietduoc / boyte.
        domain_tag = re.sub(
            r"[^a-z0-9]+", "_", (self.base_domain or "site").lower()
        ).strip("_")
        self.state = CrawlState(
            output_root=self.output_root,
            events_filename=f"events_soyte_csv_{domain_tag}_{_short_hash(self.base_domain)}.jsonl",
        )

    async def _crawl_one(
        self, url: str
    ) -> Tuple[Optional[PageItem], list[str], Optional[str]]:
        """Giống lớp cha nhưng `entry_html` = inner HTML của `#MainContent` (id)."""
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

            entry_html = extract_inner_html_by_element_id(
                html, self.content_element_id
            )
            if not (entry_html or "").strip():
                return None, [], "no_maincontent"

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


def _parse_args(argv: list[str]) -> dict:
    import argparse

    p = argparse.ArgumentParser(
        description="Crawl medinet.hochiminhcity.gov.vn → CSV trong data/ (local, có resume, log)."
    )
    p.add_argument("--start-url", default=DEFAULT_START_URL)
    p.add_argument(
        "--max-pages", type=int, default=DEFAULT_MAX_PAGES, help="0 = không giới hạn"
    )
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    p.add_argument(
        "--browser-concurrency",
        type=int,
        default=DEFAULT_BROWSER_CONCURRENCY,
        help="Giới hạn số tác vụ Playwright song song (giảm lỗi GC heap).",
    )
    p.add_argument(
        "--recycle-every",
        type=int,
        default=DEFAULT_RECYCLE_EVERY,
        help="Recycle browser mỗi N lần arun (0 = tắt).",
    )
    p.add_argument("--page-timeout-ms", type=int, default=DEFAULT_PAGE_TIMEOUT_MS)
    p.add_argument(
        "--output-root",
        default=DEFAULT_OUTPUT_ROOT,
        help="Thư mục chứa CSV, state/, logs/",
    )
    p.add_argument(
        "--connect-error-retry-limit",
        type=int,
        default=DEFAULT_CONNECT_ERROR_RETRY_LIMIT,
    )
    p.add_argument("--no-resume", action="store_true")
    p.add_argument(
        "--retry-failed-on-resume",
        action="store_true",
        help="Khi resume, đưa lại URL đã failed vào hàng đợi.",
    )
    p.add_argument(
        "--allow-domain",
        action="append",
        default=[],
        help="Thêm domain được phép (lặp lại được).",
    )
    p.add_argument(
        "--content-element-id",
        default=DEFAULT_CONTENT_ELEMENT_ID,
        help='id DOM chứa nội dung chính (mặc định MainContent, kiểu ASP.NET).',
    )
    return vars(p.parse_args(argv))


async def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    out = args["output_root"]
    Path(out).mkdir(parents=True, exist_ok=True)
    _setup_logging(out)

    scraper = CrawlSoyteScraper(
        start_url=(args["start_url"] or "").strip(),
        max_pages=(
            (args["max_pages"] or None) if int(args["max_pages"] or 0) > 0 else None
        ),
        concurrency=args["concurrency"],
        browser_concurrency=args["browser_concurrency"],
        recycle_every=args["recycle_every"],
        page_timeout_ms=args["page_timeout_ms"],
        output_root=out,
        resume=not bool(args["no_resume"]),
        retry_failed_on_resume=bool(args["retry_failed_on_resume"]),
        allow_domains=args["allow_domain"] or [],
        connect_error_retry_limit=args["connect_error_retry_limit"],
        content_element_id=str(args["content_element_id"] or DEFAULT_CONTENT_ELEMENT_ID),
    )

    logger.info(
        "Soyte crawl: start_url=%s output_root=%s resume=%s state=%s",
        scraper.start_url,
        out,
        scraper.resume,
        scraper.state.events_path,
    )
    await scraper.run()
    logger.info(
        "Hoàn tất. CSV: %s | Log: %s | State (resume): %s",
        Path(out) / CSV_PAGES,
        Path(out) / LOG_REL_PATH,
        scraper.state.events_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
