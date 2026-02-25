import asyncio
import csv
import logging
from datetime import datetime
from urllib.parse import urljoin, urlparse
from crawl4ai import AsyncWebCrawler, CrawlResult
from crawl4ai.extraction_strategy import LLMExtractionStrategy

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CrawlAIBenhVienScraper:
    """
    Scraper using crawl4ai with LLM extraction for benhvienanbinh.vn
    """

    def __init__(self, max_pages=None):
        self.base_url = "https://benhvienanbinh.vn/"
        self.base_domain = "benhvienanbinh.vn"
        self.visited_urls = set()
        self.items_data = []
        self.max_pages = max_pages
        self.crawler = None

    def is_same_domain(self, url):
        """Check if URL belongs to the same domain"""
        try:
            parsed = urlparse(url)
            return self.base_domain in parsed.netloc
        except Exception:
            return False

    async def crawl_page(self, url):
        """Crawl a single page using crawl4ai with LLM extraction"""
        if url in self.visited_urls:
            return []

        if self.max_pages and len(self.visited_urls) >= self.max_pages:
            return []

        self.visited_urls.add(url)
        logger.info(f"Crawling: {url} ({len(self.visited_urls)}/{self.max_pages})")

        try:
            # Define extraction schema with LLM
            extraction_strategy = LLMExtractionStrategy(
                provider="openai",
                api_token=None,  # Will use OPENAI_API_KEY env var
                schema={
                    "type": "object",
                    "properties": {
                        "title": {
                            "type": "string",
                            "description": "Article or page title",
                        },
                        "date": {
                            "type": "string",
                            "description": "Publication or update date",
                        },
                        "author_source": {
                            "type": "string",
                            "description": "Author name or source",
                        },
                        "email_phone": {
                            "type": "string",
                            "description": "Email address or phone number for contact",
                        },
                        "address_city": {
                            "type": "string",
                            "description": "Physical address or city location",
                        },
                        "services_specialties": {
                            "type": "string",
                            "description": "Services offered or medical specialties",
                        },
                    },
                    "required": ["title"],
                },
            )

            result = await self.crawler.arun(
                url=url, extraction_strategy=extraction_strategy, bypass_cache=True
            )

            if result.success:
                extracted_data = result.extracted_content

                # Parse extracted data
                if isinstance(extracted_data, list):
                    for item in extracted_data:
                        item["url"] = url
                        self.items_data.append(item)
                elif isinstance(extracted_data, dict):
                    extracted_data["url"] = url
                    self.items_data.append(extracted_data)

                # Extract links for crawling
                links = result.links.get("internal", []) if result.links else []
                return links
            else:
                logger.warning(f"Failed to crawl {url}: {result.error_message}")
                return []

        except Exception as e:
            logger.error(f"Error crawling {url}: {e}")
            return []

    async def crawl_recursive(self):
        """Recursively crawl pages starting from base URL"""
        queue = [self.base_url]

        while queue and (not self.max_pages or len(self.visited_urls) < self.max_pages):
            url = queue.pop(0)

            if url not in self.visited_urls and self.is_same_domain(url):
                links = await self.crawl_page(url)
                for link in links:
                    if link not in self.visited_urls and self.is_same_domain(link):
                        if not any(
                            skip in link.lower()
                            for skip in ["logout", "login", "admin", "#"]
                        ):
                            queue.append(link)

    def export_to_csv(self, filename="benhvien_data_crawl4ai.csv"):
        """Export collected items to CSV"""
        if not self.items_data:
            logger.warning("No data to export")
            return

        fieldnames = [
            "title",
            "date",
            "author_source",
            "email_phone",
            "address_city",
            "services_specialties",
            "url",
        ]

        try:
            with open(filename, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(self.items_data)
            logger.info(f"Exported {len(self.items_data)} items to {filename}")
        except Exception as e:
            logger.error(f"Error exporting to CSV: {e}")

    async def run(self):
        """Run the scraper"""
        try:
            async with AsyncWebCrawler() as crawler:
                self.crawler = crawler
                await self.crawl_recursive()

            self.export_to_csv()

            print(f"\n=== Crawl4AI Summary ===")
            print(f"Total items collected: {len(self.items_data)}")
            print(f"Total URLs visited: {len(self.visited_urls)}")
            print(f"Exported to: benhvien_data_crawl4ai.csv")

        except Exception as e:
            logger.error(f"Fatal error: {e}")


async def main():
    # max_pages=None for unlimited, or set a number like max_pages=50
    scraper = CrawlAIBenhVienScraper(max_pages=None)
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(main())
