import argparse, csv, re, time, yaml
import concurrent.futures
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse
import re
import unicodedata
import requests
from bs4 import BeautifulSoup

def load_yaml(path: Path) -> dict:
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f)

def get_soup(url: str, headers: dict, timeout: int = 30) -> BeautifulSoup:
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def make_absolute(href: str, base: str) -> str:
    return href if urlparse(href).netloc else urljoin(base, href)

def extract_field(soup: BeautifulSoup, cfg: dict, base_url: str) -> str:
    """Return one column value according to its YAML rules."""
    if "selector" in cfg:
        nodes = soup.select(cfg["selector"])
        exclude = {s.lower() for s in cfg.get("exclude_equals", [])}
        joiner  = cfg.get("join_with", " ")
        for n in nodes:
            txt = n.get_text(joiner, strip=True)
            if txt.lower() not in exclude:
                return txt
        return ""

    if "regex" in cfg:
        raw_text = soup.get_text(" ", strip=True)
        # normalise ALL whitespace to a single ASCII space
        norm = re.sub(r"\s+", " ", raw_text, flags=re.UNICODE)
        m = re.search(cfg["regex"], norm)
        if not m:
            return ""
        value = m.group(0)
        fmt = cfg.get("strptime")
        if fmt:
            try:
                value = datetime.strptime(value, fmt).isoformat()
            except ValueError:
                pass
        return value
    return ""

def process_item(url: str, headers: dict, fields_cfg: dict, columns: list[str], base_url: str, delay: float) -> dict:
    """Fetches a single URL, extracts data, and handles rate limiting."""
    try:
        # Apply delay *before* the request to rate-limit individually
        time.sleep(delay)
        soup = get_soup(url, headers)
        row = [extract_field(soup, fields_cfg[name], base_url) for name in columns]
        title = row[columns.index("title")] if "title" in columns else url
        date = row[columns.index("date")] if "date" in columns else ""
        return {"status": "success", "url": url, "row": row, "title": title, "date": date}
    except requests.exceptions.RequestException as e:
        # Handle network/HTTP errors specifically
        return {"status": "error", "url": url, "error": f"Request failed: {e}"}
    except Exception as e:
        # Handle other potential errors during processing
        return {"status": "error", "url": url, "error": f"Processing failed: {e}"}

def main():
    ap = argparse.ArgumentParser(description="Scrape item details from a website.")
    ap.add_argument("--config", required=True, type=Path, help="Path to the YAML configuration file.")
    ap.add_argument("--output", default="output.csv", type=Path, help="Path to the output CSV file.")
    ap.add_argument("--request_delay", default=0.2, type=float, help="Delay in seconds between requests per worker.")
    ap.add_argument("--workers", default=5, type=int, help="Number of concurrent workers for fetching.")
    args = ap.parse_args()
    cfg = load_yaml(args.config)
    headers = {"User-Agent": cfg.get("user_agent", "scraper/2.0")}
    base_url = cfg.get("base_url", cfg["start_url"])
    delay = args.request_delay
    max_workers = args.workers
    fields_cfg = cfg["fields"]
    columns = cfg["output_columns"]

    print(f"Fetching start page: {cfg['start_url']}")
    try:
        start_soup = get_soup(cfg["start_url"], headers)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching start URL: {e}")
        return # Exit if start page fails

    item_urls = {
        make_absolute(a["href"], base_url)
        for a in start_soup.select(cfg["item_url_selector"])
    }
    item_urls = sorted(list(item_urls)) # Ensure it's a list for consistent ordering
    total_items = len(item_urls)
    print(f"{total_items} items found to process.")

    processed_count = 0
    success_count = 0
    error_count = 0

    # Use ThreadPoolExecutor for concurrency
    with args.output.open("w", newline="", encoding="utf-8") as fp, \
         concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:

        writer = csv.writer(fp)
        writer.writerow(columns) # Write header row

        # Submit tasks to the executor
        future_to_url = {
            executor.submit(process_item, url, headers, fields_cfg, columns, base_url, delay): url
            for url in item_urls
        }

        print(f"Processing items with {max_workers} workers (delay={delay}s/request)...")
        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_url):
            processed_count += 1
            url = future_to_url[future]
            try:
                result = future.result()
                if result["status"] == "success":
                    writer.writerow(result["row"])
                    success_count += 1
                    print(f"[{processed_count}/{total_items} | OK] {result.get('title', url)} {result.get('date','')}")
                else:
                    error_count += 1
                    print(f"[{processed_count}/{total_items} | ERR] {url} - {result['error']}")

            except Exception as exc:
                # Catch potential errors from future.result() itself
                error_count += 1
                print(f"[{processed_count}/{total_items} | EXC] {url} generated an exception: {exc}")

    print("-" * 20)
    print(f"Processing complete.")
    print(f"  Total items:   {total_items}")
    print(f"  Successful:    {success_count}")
    print(f"  Errors:        {error_count}")
    print(f"Saved → {args.output.resolve()}")

if __name__ == "__main__":
    main()