import asyncio
import csv
import json
import logging
import re
import pandas as pd
from datetime import datetime
from urllib.parse import urljoin, urlparse
from crawl4ai import AsyncWebCrawler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
    ):
        # Cho phép 1 file hoặc nhiều file CSV
        self.input_csv = input_csv if isinstance(input_csv, list) else [input_csv]
        self.base_domain = "benhvienanbinh.vn"
        self.max_pages = max_pages
        self.concurrency = concurrency  # Số URL crawl song song
        self.crawler = None
        self.visited_urls = set()
        self.content_data = []  # {url, title, content, link (video+links), navigation}
        self.urls_to_process = []  # list of (url, title_from_csv or None)
        self.home_url = None
        self._semaphore = None
        self._csv_file = None
        self._csv_writer = None
        self._jsonl_file = None
        # Cây nav tăng dần khi ghi từng kết quả (để gán nav_id cho CSV/JSONL)
        self._nav_key_to_id: dict[tuple[str, int], int] = {}
        self._nav_next_id = 2

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
        link_str = " | ".join(result.get("link", [])) if isinstance(result.get("link"), list) else str(result.get("link", ""))
        if self._csv_writer:
            self._csv_writer.writerow([
                result.get("title", ""),
                result.get("url", ""),
                result.get("nav_id", ""),
                result.get("content", ""),
                link_str,
                result.get("navigation", ""),
                result.get("timestamp", ""),
            ])
            self._csv_file.flush()
        if self._jsonl_file:
            self._jsonl_file.write(json.dumps(result, ensure_ascii=False) + "\n")
            self._jsonl_file.flush()

    async def _fetch_one(self, url: str, title_from_csv: str = None) -> dict | None:
        """Một task crawl 1 URL (dùng semaphore để giới hạn đồng thời)."""
        if url in self.visited_urls:
            return None
        self.visited_urls.add(url)

        async with self._semaphore:
            logger.info(
                f"Fetching: {url} ({len(self.visited_urls)}/{len(self.urls_to_process)})"
            )
            result = await self._do_fetch(url, title_from_csv)
            if result:
                self._write_result_immediately(result)
            return result

    async def _do_fetch(self, url: str, title_from_csv: str = None) -> dict | None:
        """Thực hiện crawl 1 URL (gọi từ _fetch_one)."""
        try:
            result = await self.crawler.arun(
                url=url, bypass_cache=True, wait_until="network_idle"
            )

            if result.success:
                html = result.html or ""
                # Title: ưu tiên từ CSV, không có thì lấy từ page
                title = title_from_csv
                if not title:
                    title = self.extract_title(result.markdown or "")
                if not title:
                    title = self.extract_title_from_html(html)

                # Raw content = toàn bộ markdown (đoạn text content)
                raw_content = result.markdown or html or ""

                # Video và link trong page -> cột link (list URL)
                page_links = self.extract_links_and_videos(html, url)

                # Navigation: trang home thì dạng tree node
                is_home = self.normalize_path(url) == self.home_url
                navigation = self.extract_navigation_tree(html, url) if is_home else self.extract_navigation(html)

                return {
                    "url": url,
                    "title": title,
                    "content": raw_content,
                    "link": page_links,
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
                    links = item.get("link", [])
                    if links:
                        f.write(f"**Link (video/links):**\n")
                        for L in links:
                            f.write(f"  - {L}\n")
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
                links = item.get("link", [])
                link_str = " | ".join(links) if isinstance(links, list) else str(links)
                rows.append({
                    "title": item["title"],
                    "url": item["url"],
                    "content": item["content"],
                    "link": link_str,
                    "navigation": item["navigation"],
                    "timestamp": item["timestamp"],
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
        - Sheet Content: title, url, nav_id, content_preview, full_content, link, timestamp.
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
                links = item.get("link", [])
                link_str = "\n".join(links) if isinstance(links, list) else str(links)
                content_rows.append({
                    "title": item["title"],
                    "url": item["url"],
                    "nav_id": item.get("nav_id", ""),
                    "navigation": item.get("navigation", ""),
                    "link": link_str,
                    "content_preview": (
                        item["content"][:500] + "..."
                        if len(item["content"]) > 500
                        else item["content"]
                    ),
                    "full_content": item["content"],
                    "timestamp": item["timestamp"],
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

    async def run(self):
        """Run the content fetcher (asyncio song song, giới hạn bởi concurrency)."""
        if not self.load_urls_from_csv():
            return

        to_crawl = [
            (url, title_csv)
            for url, title_csv in self.urls_to_process
            if self.is_same_domain(url)
        ]
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
            self._csv_writer.writerow(["title", "url", "nav_id", "content", "link", "navigation", "timestamp"])
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
            return self.base_domain in parsed.netloc
        except Exception:
            return False


async def main():
    # Một file: input_csv="benhvien_data.csv"
    # Nhiều file cùng lúc: input_csv=["file1.csv", "file2.csv", "file3.csv"]
    scraper = CrawlAIBenhVienScraperSimple(
        input_csv="benhvien_data.csv",  # hoặc ["a.csv", "b.csv", "c.csv"]
        max_pages=None,
        concurrency=10,
    )
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(main())
