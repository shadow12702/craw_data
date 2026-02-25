import csv
import logging
from datetime import datetime
from urllib.parse import urljoin, urlparse
from scrapling.spiders import Spider, Request, Response

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BenhVienAnBinhSpider(Spider):
    """
    Spider for crawling benhvienanbinh.vn
    Extracts: title, date, author/source, email/phone, address/city, services/specialties
    """

    name = "benhvien_anbinh"
    start_urls = ["https://benhvienanbinh.vn/"]
    concurrent_requests = 5
    download_delay = 1

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.visited_urls = set()
        self.base_domain = "benhvienanbinh.vn"
        self.items_data = []

    def is_same_domain(self, url):
        """Check if URL belongs to the same domain"""
        try:
            parsed = urlparse(url)
            return self.base_domain in parsed.netloc
        except Exception:
            return False

    async def parse(self, response: Response):
        """Parse main pages and extract links"""
        if response.url in self.visited_urls:
            return

        self.visited_urls.add(response.url)
        logger.info(f"Parsing: {response.url}")

        # Extract article/content pages
        articles = response.css('article, .post, .blog-post, [class*="article"]')

        if articles:
            for article in articles:
                try:
                    item = self.extract_item(article, response)
                    if item:
                        self.items_data.append(item)
                        yield item
                except Exception as e:
                    logger.warning(f"Error extracting article: {e}")

        # Extract links to follow (same domain only)
        links = response.css("a::attr(href)").getall()
        for link in links:
            absolute_url = urljoin(response.url, link)

            if (
                self.is_same_domain(absolute_url)
                and absolute_url not in self.visited_urls
            ):
                # Skip common non-content pages
                if not any(
                    skip in absolute_url.lower()
                    for skip in ["logout", "login", "admin", "#"]
                ):
                    yield Request(absolute_url, callback=self.parse)

    def extract_item(self, element, parent_response):
        """Extract item data from an element"""
        try:
            item = {}

            # Title
            title_selectors = [
                "h1::text",
                "h2::text",
                "h3::text",
                ".title::text",
                ".heading::text",
                ".post-title::text",
            ]
            title = None
            for selector in title_selectors:
                title = element.css(selector).get()
                if title:
                    break
            item["title"] = (title or "").strip()

            # Date
            date_text = element.css(
                '[class*="date"], time::attr(datetime), .post-date::text'
            ).get()
            item["date"] = (
                (date_text or "").strip() if date_text else datetime.now().isoformat()
            )

            # Author/Source
            author = element.css('[class*="author"], .by::text, .source::text').get()
            item["author_source"] = (author or "").strip()

            # Email/Phone
            email = element.css('a[href*="mailto"]::attr(href)').get()
            phone = element.css('[class*="phone"]::text').get()
            item["email_phone"] = ""
            if email:
                item["email_phone"] = email.replace("mailto:", "")
            if phone:
                item["email_phone"] += " " + phone.strip()
            item["email_phone"] = item["email_phone"].strip()

            # Address/City
            address = element.css('[class*="address"], [class*="location"]::text').get()
            item["address_city"] = (address or "").strip()

            # Services/Specialties
            services = element.css(
                '[class*="service"], [class*="specialty"], [class*="depart"]::text'
            ).getall()
            item["services_specialties"] = ", ".join(
                [s.strip() for s in services if s.strip()]
            )

            # Page URL
            item["url"] = parent_response.url

            # Skip if no meaningful content
            if not item.get("title") and not item.get("services_specialties"):
                return None

            return item
        except Exception as e:
            logger.warning(f"Error in extract_item: {e}")
            return None

    def export_to_csv(self, filename="benhvien_data.csv"):
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
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(self.items_data)
            logger.info(f"Exported {len(self.items_data)} items to {filename}")
        except Exception as e:
            logger.error(f"Error exporting to CSV: {e}")


def main():
    spider = BenhVienAnBinhSpider()
    result = spider.start()

    # Export to CSV
    spider.export_to_csv("benhvien_data.csv")

    print(f"\n=== Crawl Summary ===")
    print(f"Total items collected: {len(spider.items_data)}")
    print(f"Total URLs visited: {len(spider.visited_urls)}")
    print(f"Result status: {result.status}")
    print(f"Exported to: benhvien_data.csv")


if __name__ == "__main__":
    main()
