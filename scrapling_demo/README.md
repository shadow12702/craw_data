Scrapling demo

This is a small demo project showing how to use Scrapling to scrape data.

Setup (Windows cmd):

```bat
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python scrapling_demo\scraper.py
```

Alternative (install Scrapling directly):

```bat
pip install "scrapling[fetchers]"
python scrapling_demo\scraper.py
```

The script saves results to `quotes.json` in the current working directory.

## Healthcare Scraper Examples

### Using Scrapling (CSS/XPath selectors)

```bat
python scrapling_demo\benhvien_anbinh_scraper.py
```

Output: `benhvien_data.csv` (concurrent, fast, no API key required)

### Using crawl4ai (LLM-based extraction)

crawl4ai uses AI to intelligently extract data. First, install and set OpenAI API key:

```bat
pip install crawl4ai openai
set OPENAI_API_KEY=sk-your-key-here
python scrapling_demo\crawl4ai_benhvien_scraper.py
```

Output: `benhvien_data_crawl4ai.csv` (uses LLM for smart extraction, requires API key)

Windows notes:

- Requires Python 3.10 or newer.
- If you plan to use browser-backed fetchers (Playwright/StealthyFetcher), after installing extras run:

```bat
scrapling install
```

    This downloads browsers and system dependencies and may take several minutes and disk space.

Docker is optional:

- The included `Dockerfile` provides a reproducible container with `scrapling[fetchers]` preinstalled. Use it when you want a consistent runtime or for deployment/CI.
- If you prefer developing on Windows, Docker is not required — the venv + pip steps above are sufficient.
