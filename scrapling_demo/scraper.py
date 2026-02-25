import json
from scrapling.fetchers import Fetcher


def main():
    # Simple demo that scrapes quotes from quotes.toscrape.com
    page = Fetcher.get("https://quotes.toscrape.com/")
    quotes = []

    for q in page.css(".quote"):
        quotes.append(
            {
                "text": q.css(".text::text").get(),
                "author": q.css(".author::text").get(),
            }
        )

    with open("quotes.json", "w", encoding="utf-8") as f:
        json.dump(quotes, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(quotes)} quotes to quotes.json")


if __name__ == "__main__":
    main()
