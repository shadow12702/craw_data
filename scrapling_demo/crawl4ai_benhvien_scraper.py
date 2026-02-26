import asyncio
import csv
import hashlib
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import ParseResult, urljoin, urlparse, urlunparse
from urllib.request import Request, urlopen

try:
    from crawl4ai import AsyncWebCrawler, CacheMode, CrawlerRunConfig
except Exception:  # pragma: no cover
    from crawl4ai import AsyncWebCrawler  # type: ignore

    CacheMode = None  # type: ignore
    CrawlerRunConfig = None  # type: ignore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_SKIP_URL_CONTAINS = (
    "logout",
    "login",
    "wp-admin",
    "/admin",
    "mailto:",
    "tel:",
)

_SKIP_EXTENSIONS = (
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
    ".css",
    ".js",
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_filename(s: str) -> str:
    s = re.sub(r"[^\w\-\. ]+", "_", s, flags=re.UNICODE).strip()
    s = re.sub(r"\s+", " ", s)
    return s[:180] or "output"


def _short_hash(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()[:10]


def _coerce_link(link_item) -> Optional[str]:
    if isinstance(link_item, str):
        return link_item
    if isinstance(link_item, dict):
        href = link_item.get("href") or link_item.get("url")
        return href if isinstance(href, str) else None
    return None


def normalize_url(url: str, base_url: str) -> Optional[str]:
    if not url:
        return None

    url = url.strip()
    if url.startswith("//"):
        url = "https:" + url

    joined = urljoin(base_url, url)
    parsed = urlparse(joined)
    if parsed.scheme not in ("http", "https"):
        return None

    netloc = parsed.netloc.lower().strip(".")
    path = re.sub(r"/{2,}", "/", parsed.path or "/")
    normalized = ParseResult(
        scheme=parsed.scheme.lower(),
        netloc=netloc,
        path=path,
        params="",
        query=parsed.query or "",
        fragment="",
    )
    return urlunparse(normalized)


def is_related_domain(url: str, base_domain: str, extra_allow_domains: Iterable[str]) -> bool:
    try:
        netloc = urlparse(url).netloc.lower().strip(".")
    except Exception:
        return False

    allow = {base_domain.lower().strip(".")}
    allow.update(d.lower().strip(".") for d in extra_allow_domains if d)

    if netloc in allow:
        return True
    for d in allow:
        if netloc.endswith("." + d):
            return True
    return False


def extract_title_from_markdown(markdown: str) -> str:
    for line in (markdown or "").splitlines():
        line = line.strip()
        if line.startswith("#") and len(line) > 1:
            return line.lstrip("#").strip()
    return ""


def extract_title_from_html(html: str) -> str:
    html = html or ""
    for pattern in (
        r"<h1[^>]*>\s*([^<]+?)\s*</h1>",
        r"<title[^>]*>\s*([^<]+?)\s*</title>",
        r"<h2[^>]*>\s*([^<]+?)\s*</h2>",
    ):
        m = re.search(pattern, html, flags=re.IGNORECASE)
        if m:
            return re.sub(r"\s+", " ", m.group(1)).strip()
    return ""


def strip_media_from_markdown(markdown: str) -> str:
    md = markdown or ""
    md = re.sub(r"!\[[^\]]*\]\([^)]+\)", "", md)
    md = re.sub(r"<img[^>]*>", "", md, flags=re.IGNORECASE)
    md = re.sub(r"<video[\s\S]*?</video>", "", md, flags=re.IGNORECASE)
    md = re.sub(r"<iframe[\s\S]*?</iframe>", "", md, flags=re.IGNORECASE)
    md = re.sub(r"\n{3,}", "\n\n", md)
    return md.strip()


def _extract_tag_attrs(tag_html: str) -> dict[str, str]:
    attrs: dict[str, str] = {}
    for m in re.finditer(r"(\w+)\s*=\s*['\"]([^'\"]+)['\"]", tag_html):
        attrs[m.group(1).lower()] = m.group(2).strip()
    return attrs


@dataclass(frozen=True)
class MediaItem:
    url: str
    name: str  # caption/alt/title; "none" if missing


def extract_media(markdown: str, html: str, page_url: str) -> tuple[list[MediaItem], list[MediaItem]]:
    md = markdown or ""
    h = html or ""

    images: list[MediaItem] = []
    videos: list[MediaItem] = []

    for m in re.finditer(r"!\[([^\]]*)\]\(([^)]+)\)", md):
        alt = (m.group(1) or "").strip()
        raw = (m.group(2) or "").strip().strip('"').strip("'")
        nu = normalize_url(raw, page_url)
        if nu:
            images.append(MediaItem(url=nu, name=alt or "none"))

    for m in re.finditer(r"<img\b[^>]*>", h, flags=re.IGNORECASE):
        tag = m.group(0)
        attrs = _extract_tag_attrs(tag)
        src = attrs.get("src") or ""
        nu = normalize_url(src, page_url)
        if not nu:
            continue
        name = (attrs.get("alt") or attrs.get("title") or "").strip() or "none"
        images.append(MediaItem(url=nu, name=name))

    for m in re.finditer(r"<(?:video|source)\b[^>]*>", h, flags=re.IGNORECASE):
        tag = m.group(0)
        attrs = _extract_tag_attrs(tag)
        src = attrs.get("src") or ""
        nu = normalize_url(src, page_url)
        if not nu:
            continue
        name = (attrs.get("title") or attrs.get("aria-label") or "").strip() or "none"
        videos.append(MediaItem(url=nu, name=name))

    for m in re.finditer(r"<iframe\b[^>]*>", h, flags=re.IGNORECASE):
        tag = m.group(0)
        attrs = _extract_tag_attrs(tag)
        src = attrs.get("src") or ""
        nu = normalize_url(src, page_url)
        if not nu:
            continue
        name = (attrs.get("title") or attrs.get("aria-label") or "").strip() or "none"
        videos.append(MediaItem(url=nu, name=name))

    for m in re.finditer(
        r"(https?://[^\s)\"']+\.(?:mp4|webm|m3u8))(?:\?[^\s)\"']*)?",
        md,
        flags=re.IGNORECASE,
    ):
        nu = normalize_url(m.group(0), page_url)
        if nu:
            videos.append(MediaItem(url=nu, name="none"))

    img_map: dict[str, str] = {}
    for it in images:
        img_map.setdefault(it.url, it.name or "none")
    vid_map: dict[str, str] = {}
    for it in videos:
        vid_map.setdefault(it.url, it.name or "none")

    images_out = [MediaItem(url=u, name=n or "none") for u, n in img_map.items()]
    videos_out = [MediaItem(url=u, name=n or "none") for u, n in vid_map.items()]
    return images_out, videos_out


@dataclass
class PageItem:
    url: str
    title: str
    markdown: str
    images: list[MediaItem]
    videos: list[MediaItem]
    crawled_at: str


class StreamingExporter:
    def __init__(self, output_root: str = "data_craw"):
        self.root = Path(output_root)
        self.content_dir = self.root / "content"
        self.image_dir = self.root / "image"
        self.video_dir = self.root / "video"
        self.videos_csv = self.video_dir / "videos.csv"

        self.content_dir.mkdir(parents=True, exist_ok=True)
        self.image_dir.mkdir(parents=True, exist_ok=True)
        self.video_dir.mkdir(parents=True, exist_ok=True)

        self._csv_lock = asyncio.Lock()
        self._image_map_lock = asyncio.Lock()
        self._downloaded_images: dict[str, str] = {}  # url -> filename

        if not self.videos_csv.exists() or self.videos_csv.stat().st_size == 0:
            with open(self.videos_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=[
                        "page_id",
                        "page_title",
                        "page_url",
                        "video_name",
                        "video_url",
                        "crawled_at",
                    ],
                )
                writer.writeheader()

    def _page_id(self, page_url: str) -> str:
        return _short_hash(page_url)

    def _content_path(self, page: PageItem) -> Path:
        page_id = self._page_id(page.url)
        title_safe = _safe_filename(page.title)[:90]
        return self.content_dir / f"{page_id}__{title_safe}.md"

    def _download_image_sync(self, url: str, dst_path: Path) -> bool:
        if dst_path.exists() and dst_path.stat().st_size > 0:
            return True

        dst_path.parent.mkdir(parents=True, exist_ok=True)
        headers = {"User-Agent": "Mozilla/5.0 (compatible; benhvien-crawler/1.0)"}
        req = Request(url, headers=headers)

        try:
            with urlopen(req, timeout=30) as resp, open(dst_path, "wb") as f:
                f.write(resp.read())
            return True
        except (HTTPError, URLError, TimeoutError) as e:
            logger.warning(f"Image download failed: {url} -> {dst_path.name} ({e})")
            return False
        except Exception as e:
            logger.warning(f"Image download error: {url} ({e})")
            return False

    async def _download_one_image(self, page_id: str, idx: int, img: MediaItem) -> str:
        async with self._image_map_lock:
            cached = self._downloaded_images.get(img.url)
        if cached:
            return f"- {img.name or 'none'}: `{cached}` ({img.url})"

        parsed = urlparse(img.url)
        ext = Path(parsed.path).suffix.lower()
        if ext not in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg"):
            ext = ".jpg"

        name_safe = _safe_filename(img.name or "none")[:60] or "none"
        img_filename = f"{page_id}__{idx:03d}__{name_safe}{ext}"
        img_path = self.image_dir / img_filename

        ok = await asyncio.to_thread(self._download_image_sync, img.url, img_path)
        if ok:
            async with self._image_map_lock:
                self._downloaded_images.setdefault(img.url, img_filename)
            return f"- {img.name or 'none'}: `{img_filename}` ({img.url})"
        return f"- {img.name or 'none'}: (download_failed) ({img.url})"

    async def export_page(self, page: PageItem) -> None:
        page_id = self._page_id(page.url)
        content_path = self._content_path(page)

        if not content_path.exists():
            with open(content_path, "w", encoding="utf-8") as f_md:
                f_md.write(f"# {page.title}\n\n")
                f_md.write(f"- URL: {page.url}\n")
                f_md.write(f"- Crawled at: {page.crawled_at}\n\n")
                f_md.write("---\n\n")
                f_md.write((page.markdown or "").strip() + "\n\n")
                f_md.write("---\n\n")
                f_md.write("## Image files\n\n")
                f_md.write("(downloaded to `data_craw/image/`)\n\n")
                f_md.write("## Videos\n\n")
                f_md.write("(listed in `data_craw/video/videos.csv`)\n")

        # Download images (I/O bound) in parallel
        image_lines: list[str] = []
        if page.images:
            sem = asyncio.Semaphore(8)

            async def _guarded(i: int, it: MediaItem):
                async with sem:
                    return await self._download_one_image(page_id, i, it)

            image_lines = await asyncio.gather(*(_guarded(i, it) for i, it in enumerate(page.images, 1)))

        with open(content_path, "a", encoding="utf-8") as f_md:
            f_md.write("\n---\n\n### Images\n\n")
            if image_lines:
                f_md.write("\n".join(image_lines) + "\n")
            else:
                f_md.write("- none\n")
            f_md.write("\n### Videos\n\n")
            if page.videos:
                f_md.write("- (see CSV) `data_craw/video/videos.csv`\n")
            else:
                f_md.write("- none\n")

        if page.videos:
            async with self._csv_lock:
                with open(self.videos_csv, "a", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(
                        f,
                        fieldnames=[
                            "page_id",
                            "page_title",
                            "page_url",
                            "video_name",
                            "video_url",
                            "crawled_at",
                        ],
                    )
                    for v in page.videos:
                        writer.writerow(
                            {
                                "page_id": page_id,
                                "page_title": page.title,
                                "page_url": page.url,
                                "video_name": v.name or "none",
                                "video_url": v.url,
                                "crawled_at": page.crawled_at,
                            }
                        )


class CrawlAIBenhVienScraper:
    """
    Crawl https://benhvienanbinh.vn/ (and related domains) and export immediately:
    - data_craw/content: 1 page = 1 .md file (text in markdown)
    - data_craw/image: download images, name from caption/alt/title (or 'none')
    - data_craw/video/videos.csv: append all video URLs
    """

    def __init__(
        self,
        start_url: str = "https://benhvienanbinh.vn/",
        max_pages: Optional[int] = None,
        concurrency: int = 6,
        allow_domains: Optional[list[str]] = None,
        page_timeout_ms: int = 300_000,
        output_root: str = "data_craw",
    ):
        self.start_url = start_url
        host = urlparse(start_url).netloc.lower().strip(".")
        if host.startswith("www."):
            host = host[4:]
        self.base_domain = host or "benhvienanbinh.vn"

        self.max_pages = max_pages
        self.concurrency = max(1, int(concurrency))
        self.allow_domains = allow_domains or []
        self.page_timeout_ms = int(page_timeout_ms)
        self.output_root = output_root

        self.crawler = None
        self.exporter = StreamingExporter(output_root=self.output_root)

        self.visited_urls: set[str] = set()
        self.queued_urls: set[str] = set()

    def _should_skip_url(self, url: str) -> bool:
        u = url.lower()
        if any(s in u for s in _SKIP_URL_CONTAINS):
            return True
        parsed = urlparse(url)
        path = (parsed.path or "").lower()
        if any(path.endswith(ext) for ext in _SKIP_EXTENSIONS):
            return True
        return False

    def _is_allowed(self, url: str) -> bool:
        return is_related_domain(url, self.base_domain, self.allow_domains)

    async def _crawl_one(self, url: str) -> tuple[Optional[PageItem], list[str]]:
        if url in self.visited_urls:
            return None, []
        if self.max_pages and len(self.visited_urls) >= self.max_pages:
            return None, []
        if not self._is_allowed(url) or self._should_skip_url(url):
            return None, []

        self.visited_urls.add(url)
        logger.info(f"Crawling: {url} ({len(self.visited_urls)}/{self.max_pages or '∞'})")

        try:
            if CrawlerRunConfig is not None:
                kwargs = {"page_timeout": self.page_timeout_ms}
                if CacheMode is not None:
                    kwargs["cache_mode"] = CacheMode.BYPASS
                run_config = CrawlerRunConfig(**kwargs)
                result = await self.crawler.arun(url=url, config=run_config)
            else:
                result = await self.crawler.arun(url=url)
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None, []

        if not getattr(result, "success", False):
            msg = getattr(result, "error_message", "") or "unknown error"
            logger.warning(f"Failed to crawl {url}: {msg}")
            return None, []

        markdown = getattr(result, "markdown", "") or ""
        html = getattr(result, "html", "") or ""

        title = extract_title_from_markdown(markdown) or extract_title_from_html(html) or url
        images, videos = extract_media(markdown, html, url)
        content_md = strip_media_from_markdown(markdown) if markdown else ""

        if not content_md.strip() and html:
            content_md = html.strip()

        item = PageItem(
            url=url,
            title=title,
            markdown=content_md,
            images=images,
            videos=videos,
            crawled_at=_now_iso(),
        )

        raw_links: list[Optional[str]] = []
        links_obj = getattr(result, "links", None) or {}
        for bucket in ("internal", "external"):
            raw_links.extend(_coerce_link(x) for x in (links_obj.get(bucket) or []))

        discovered: list[str] = []
        for raw in raw_links:
            if not raw:
                continue
            nu = normalize_url(raw, url)
            if not nu:
                continue
            if self._is_allowed(nu) and not self._should_skip_url(nu):
                discovered.append(nu)

        # de-dupe preserving order
        seen: set[str] = set()
        deduped: list[str] = []
        for u in discovered:
            if u not in seen:
                seen.add(u)
                deduped.append(u)

        return item, deduped

    async def crawl_site(self) -> None:
        start = normalize_url(self.start_url, self.start_url) or self.start_url
        q: asyncio.Queue[str] = asyncio.Queue()
        await q.put(start)
        self.queued_urls.add(start)

        stop_event = asyncio.Event()

        async def worker(worker_id: int):
            while True:
                try:
                    url = await asyncio.wait_for(q.get(), timeout=2.0)
                except asyncio.TimeoutError:
                    if q.empty():
                        return
                    continue

                try:
                    if stop_event.is_set():
                        continue

                    item, discovered = await self._crawl_one(url)
                    if item:
                        await self.exporter.export_page(item)
                        if self.max_pages and len(self.visited_urls) >= self.max_pages:
                            stop_event.set()

                    for nu in discovered:
                        if stop_event.is_set():
                            break
                        if nu in self.visited_urls or nu in self.queued_urls:
                            continue
                        self.queued_urls.add(nu)
                        await q.put(nu)
                finally:
                    q.task_done()

        workers = [asyncio.create_task(worker(i + 1)) for i in range(self.concurrency)]
        await q.join()
        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

    async def run(self):
        try:
            async with AsyncWebCrawler() as crawler:
                self.crawler = crawler
                await self.crawl_site()

            print("\n=== Crawl4AI Summary ===")
            print(f"Total URLs visited: {len(self.visited_urls)}")
            print(f"Output folder: {self.output_root}")
            print(
                f"Exported to: {self.output_root}/content, {self.output_root}/image, {self.output_root}/video/videos.csv"
            )
        except Exception as e:
            logger.error(f"Fatal error: {e}")


async def main():
    scraper = CrawlAIBenhVienScraper(
        start_url="https://benhvienanbinh.vn/",
        max_pages=None,
        concurrency=6,
        allow_domains=["www.benhvienanbinh.vn"],
        page_timeout_ms=300_000,
        output_root="data_craw",
    )
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(main())
