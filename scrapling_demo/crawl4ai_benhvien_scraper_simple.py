import asyncio
import logging
import pandas as pd
from datetime import datetime
from urllib.parse import urljoin, urlparse
from pathlib import Path
from crawl4ai import AsyncWebCrawler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CrawlAIBenhVienScraperSimple:
    """
    Scraper using crawl4ai to fetch full content from URLs
    Loads URLs from benhvien_data.csv and extracts raw content (title + body text)
    Saves as markdown with navigation structure
    """

    def __init__(self, input_csv: str = "benhvien_data.csv", max_pages: int = None):
        self.input_csv = input_csv
        self.base_domain = "benhvienanbinh.vn"
        self.max_pages = max_pages
        self.crawler = None
        self.visited_urls = set()
        self.content_data = []  # Structure: {url, title, content, navigation}
        self.urls_to_process = []

    def load_urls_from_csv(self):
        """Load URLs from existing CSV file"""
        try:
            df = pd.read_csv(self.input_csv)
            self.urls_to_process = df["url"].unique().tolist()
            logger.info(
                f"Loaded {len(self.urls_to_process)} unique URLs from {self.input_csv}"
            )

            if self.max_pages:
                self.urls_to_process = self.urls_to_process[: self.max_pages]
                logger.info(f"Limited to {self.max_pages} pages")
        except FileNotFoundError:
            logger.error(f"CSV file {self.input_csv} not found")
            return False
        return True

    async def fetch_full_content(self, url: str) -> dict:
        """Fetch full content from a URL using crawl4ai"""
        if url in self.visited_urls:
            return None

        self.visited_urls.add(url)
        logger.info(
            f"Fetching: {url} ({len(self.visited_urls)}/{len(self.urls_to_process)})"
        )

        try:
            result = await self.crawler.arun(
                url=url, bypass_cache=True, wait_until="network_idle"
            )

            if result.success:
                # Extract title from markdown
                title = self.extract_title(result.markdown or "")
                if not title:
                    title = self.extract_title_from_html(result.html or "")

                # Raw content = full markdown
                raw_content = result.markdown or result.html or ""

                # Extract navigation/breadcrumb if available
                navigation = self.extract_navigation(result.html or "")

                return {
                    "url": url,
                    "title": title,
                    "content": raw_content,
                    "navigation": navigation,
                    "timestamp": datetime.now().isoformat(),
                }
            else:
                logger.warning(f"Failed to fetch {url}")
                return None

        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None

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
        """Extract navigation/breadcrumb tree-like structure"""
        import re

        navigation = []

        # Try to extract breadcrumb
        breadcrumb = re.search(
            r"<nav[^>]*>.*?<ol|<ul[^>]*>(.*?)</ol>|</ul>",
            html,
            re.DOTALL | re.IGNORECASE,
        )

        if breadcrumb:
            # Extract all <a> tags from breadcrumb
            links = re.findall(
                r'<a[^>]*href=[\'"]([^\'"]*)[\'"][^>]*>([^<]+)</a>',
                breadcrumb.group(0),
                re.IGNORECASE,
            )
            for href, text in links:
                navigation.append(f"  → {text.strip()}")

        # Also look for main menu items
        menu = re.search(
            r"<header[^>]*>.*?</header>|<nav[^>]*>.*?</nav>",
            html,
            re.DOTALL | re.IGNORECASE,
        )

        return " | ".join(navigation) if navigation else "Home"

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
                    f.write(f"**Navigation:** {item['navigation']}\n")
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

    def save_as_xlsx(self, filename: str = "benhvien_content_detailed.xlsx"):
        """Save as Excel with title + content columns"""
        if not self.content_data:
            logger.warning("No content to save")
            return

        try:
            # Prepare data for Excel (keep content shorter for readability)
            excel_data = []
            for item in self.content_data:
                excel_data.append(
                    {
                        "title": item["title"],
                        "navigation": item["navigation"],
                        "url": item["url"],
                        "content_preview": (
                            item["content"][:500] + "..."
                            if len(item["content"]) > 500
                            else item["content"]
                        ),
                        "full_content": item["content"],
                        "timestamp": item["timestamp"],
                    }
                )

            df = pd.DataFrame(excel_data)
            df.to_excel(filename, index=False, engine="openpyxl")
            logger.info(f"Saved {len(self.content_data)} pages to {filename}")
        except Exception as e:
            logger.error(f"Error saving XLSX: {e}")

    async def run(self):
        """Run the content fetcher"""
        if not self.load_urls_from_csv():
            return

        try:
            async with AsyncWebCrawler() as crawler:
                self.crawler = crawler

                for url in self.urls_to_process:
                    if not self.is_same_domain(url):
                        continue

                    content_item = await self.fetch_full_content(url)
                    if content_item:
                        self.content_data.append(content_item)

            # Save results in multiple formats
            self.save_as_markdown("benhvien_content_detailed.md")
            self.save_as_json("benhvien_content_detailed.json")
            self.save_as_xlsx("benhvien_content_detailed.xlsx")

            print(f"\n=== Crawl4AI Content Fetcher Summary ===")
            print(f"Total pages processed: {len(self.content_data)}")
            print(f"Total URLs visited: {len(self.visited_urls)}")
            print(f"\nFiles created:")
            print(f"  - benhvien_content_detailed.md (Markdown format)")
            print(f"  - benhvien_content_detailed.json (JSON format)")
            print(f"  - benhvien_content_detailed.xlsx (Excel format)")

        except Exception as e:
            logger.error(f"Fatal error: {e}")

    def is_same_domain(self, url: str) -> bool:
        """Check if URL belongs to the same domain"""
        try:
            parsed = urlparse(url)
            return self.base_domain in parsed.netloc
        except Exception:
            return False


async def main():
    # Load URLs from existing CSV (generated by benhvien_anbinh_scraper.py)
    # and fetch full content from each URL

    # You can set max_pages=20 to limit processing, or leave None for all
    scraper = CrawlAIBenhVienScraperSimple(
        input_csv="benhvien_data.csv",  # Input: CSV from first scraper
        max_pages=None,  # Set to 10-20 for testing
    )
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(main())
