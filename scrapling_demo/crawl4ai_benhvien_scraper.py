import asyncio
import hashlib
import json
import logging
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional
from urllib.parse import ParseResult, urljoin, urlparse, urlunparse

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


class PostgresWriter:
    """
    Write crawled pages to Postgres (static config from scrapling_demo/db_config.py).

    Target tables (must exist already):
      - storage_data(id UUID PK, title, content, created_at, link)
      - storage_video(id UUID PK, storage_id UUID FK, video_url)
      - storage_image(id UUID PK, storage_id UUID FK, image_url)
    """

    def __init__(self):
        try:
            from scrapling_demo.db_config import load_postgres_config
        except Exception:
            from db_config import load_postgres_config  # type: ignore

        self.cfg = load_postgres_config()
        self._sem = asyncio.Semaphore(2)  # limit concurrent DB writes

    async def start(self) -> None:
        if psycopg is None:
            raise RuntimeError("Missing psycopg. Install: pip install psycopg[binary]")
        return

    async def stop(self) -> None:
        return

    async def write(self, page: PageItem) -> None:
        async with self._sem:
            await asyncio.to_thread(self._upsert_page, page)

    @staticmethod
    def _doc_id_from_url(url: str) -> uuid.UUID:
        # Deterministic UUID so reruns update the same record
        return uuid.uuid5(uuid.NAMESPACE_URL, url)

    @staticmethod
    def _media_id(doc_id: uuid.UUID, kind: str, media_url: str) -> uuid.UUID:
        return uuid.uuid5(uuid.NAMESPACE_URL, f"{doc_id}|{kind}|{media_url}")

    def _upsert_page(self, page: PageItem) -> None:
        doc_id = self._doc_id_from_url(page.url)
        content = (page.markdown or "").strip()

        with psycopg.connect(self.cfg.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO storage_data (id, title, content, link)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE
                      SET title = EXCLUDED.title,
                          content = EXCLUDED.content,
                          link = EXCLUDED.link;
                    """,
                    (doc_id, (page.title or "")[:255], content, page.url[:500]),
                )

                if page.images:
                    cur.executemany(
                        """
                        INSERT INTO storage_image (id, storage_id, image_url)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (id) DO NOTHING;
                        """,
                        [
                            (self._media_id(doc_id, "image", img.url), doc_id, img.url[:500])
                            for img in page.images
                        ],
                    )

                if page.videos:
                    cur.executemany(
                        """
                        INSERT INTO storage_video (id, storage_id, video_url)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (id) DO NOTHING;
                        """,
                        [
                            (self._media_id(doc_id, "video", v.url), doc_id, v.url[:500])
                            for v in page.videos
                        ],
                    )
            conn.commit()

class CrawlState:
    """
    Append-only event log to support resume after stop.
    Stored at: <output_root>/state/events.jsonl
    """

    def __init__(self, output_root: str, events_filename: str = "events.jsonl"):
        self.state_dir = Path(output_root) / "state"
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.events_path = self.state_dir / events_filename
        self._lock = asyncio.Lock()

    async def record(self, event_type: str, url: str, ok: Optional[bool] = None, error: Optional[str] = None) -> None:
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

        if not self.events_path.exists():
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


class CrawlAIBenhVienScraper:
    """
    Crawl https://benhvienanbinh.vn/ (and related domains) and write to Postgres:
    - storage_data: title, content (markdown; URL is embedded at top), created_at
    - storage_image: image_url
    - storage_video: video_url
    """

    def __init__(
        self,
        start_url: str = "https://benhvienanbinh.vn/",
        max_pages: Optional[int] = None,
        concurrency: int = 6,
        allow_domains: Optional[list[str]] = None,
        page_timeout_ms: int = 300_000,
        output_root: str = "data_craw",
        resume: bool = True,
        retry_failed_on_resume: bool = False,
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
        self.resume = bool(resume)
        self.retry_failed_on_resume = bool(retry_failed_on_resume)

        self.crawler = None
        self.db_writer = PostgresWriter()
        # Use a separate state file for DB mode so old file-export runs don't block re-crawling.
        self.state = CrawlState(output_root=self.output_root, events_filename="events_db.jsonl")

        self.visited_urls: set[str] = set()  # done_ok only
        self.failed_urls: set[str] = set()
        self.processing_urls: set[str] = set()
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

    async def _crawl_one(self, url: str) -> tuple[Optional[PageItem], list[str], Optional[str]]:
        if url in self.visited_urls:
            return None, [], None
        if self.max_pages and len(self.visited_urls) >= self.max_pages:
            return None, [], "max_pages_reached"
        if not self._is_allowed(url) or self._should_skip_url(url):
            return None, [], "not_allowed_or_skipped"
        if url in self.processing_urls:
            return None, [], "already_processing"

        self.processing_urls.add(url)
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

            if not getattr(result, "success", False):
                msg = getattr(result, "error_message", "") or "unknown error"
                return None, [], f"crawl_failed: {msg}"

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

            return item, deduped, None
        except Exception as e:
            return None, [], f"fetch_error: {e}"
        finally:
            self.processing_urls.discard(url)

    async def crawl_site(self) -> None:
        start = normalize_url(self.start_url, self.start_url) or self.start_url
        q: asyncio.Queue[str] = asyncio.Queue()

        if self.resume:
            enqueued, started, done_ok, done_failed = self.state.load()
            self.visited_urls = set(done_ok)
            self.failed_urls = set(done_failed)
            self.queued_urls = set(enqueued)

            pending = (set(enqueued) | set(started)) - set(done_ok)
            if not self.retry_failed_on_resume:
                pending -= set(done_failed)

            if start not in self.queued_urls and start not in self.visited_urls:
                self.queued_urls.add(start)
                pending.add(start)
                await self.state.record("enqueue", start)

            for u in pending:
                await q.put(u)

            logger.info(
                f"Resume enabled: done_ok={len(done_ok)}, done_failed={len(done_failed)}, pending={q.qsize()}"
            )
        else:
            await q.put(start)
            self.queued_urls.add(start)
            await self.state.record("enqueue", start)

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

                    await self.state.record("start", url)
                    item, discovered, err = await self._crawl_one(url)

                    if item is not None and err is None:
                        try:
                            await self.db_writer.write(item)
                        except Exception as e:
                            self.failed_urls.add(url)
                            await self.state.record("done", url, ok=False, error=f"db_write_error: {e}")
                        else:
                            self.visited_urls.add(url)
                            await self.state.record("done", url, ok=True)
                            if self.max_pages and len(self.visited_urls) >= self.max_pages:
                                stop_event.set()
                    elif err:
                        self.failed_urls.add(url)
                        await self.state.record("done", url, ok=False, error=err)

                    for nu in discovered:
                        if stop_event.is_set():
                            break
                        if nu in self.visited_urls or nu in self.queued_urls:
                            continue
                        self.queued_urls.add(nu)
                        await self.state.record("enqueue", nu)
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
                await self.db_writer.start()
                await self.crawl_site()
                await self.db_writer.stop()

            print("\n=== Crawl4AI Summary ===")
            print(f"Total URLs visited: {len(self.visited_urls)}")
            print("Exported to Postgres tables: storage_data, storage_image, storage_video")
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
        resume=True,
        retry_failed_on_resume=False,
    )
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(main())
