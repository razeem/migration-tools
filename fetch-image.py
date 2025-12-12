#!/usr/bin/env python3
"""
fetch-image.py (certifi-enabled)

Same script as before but:
- Uses certifi CA bundle by default for requests
- Adds --insecure to disable verification (debugging only; unsafe)
- If certifi is missing, instructs user to install it
"""
from __future__ import annotations

import argparse
import csv
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Defaults (can be overridden via CLI)
INPUT_CSV = "sample.csv"
INPUT_CSV = "sample.csv"
PAGEURL_COLUMN_NAME = "PageUrl"   # case-insensitive match
IMAGE_COLUMN_NAME = "ImageURL"

DEFAULT_IMAGE_SELECTOR = "div.ms-rtestate-field img"
NUM_WORKERS = 8
REQUEST_TIMEOUT = 12
RETRY_COUNT = 2
SLEEP_BETWEEN_REQUESTS = 0.2
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; MOJImageFetcher/1.0; +https://example.com/bot)"
}

# Global flag — will be set in main() from parsed args
VERBOSE = False
VERIFY_BUNDLE: Optional[str] = None
INSECURE = False
ERROR_LOG_FILE = "fetch_errors.log"
LOG_LOCK = Lock()

def log_missing(msg: str) -> None:
    """Log missing images or failed URLs to a separate file safely."""
    with LOG_LOCK:
        try:
            with open(ERROR_LOG_FILE, "a", encoding="utf-8") as f:
                f.write(msg + "\n")
        except Exception as e:
            print(f"[ERROR] Could not write to error log: {e}", file=sys.stderr)

def log(msg: str = "", /) -> None:
    """Log only when VERBOSE is enabled."""
    if VERBOSE:
        print(msg)

def fetch_html(url: str) -> Tuple[Optional[str], Optional[str]]:
    """Fetch HTML for a URL with retry/backoff. Returns (html, final_url) or (None, None)."""
    log(f"[DEBUG] Fetching URL: {url}")
    last_exc = None
    for attempt in range(RETRY_COUNT + 1):
        try:
            # choose verify parameter
            verify_arg = False if INSECURE else (VERIFY_BUNDLE if VERIFY_BUNDLE else True)
            try:
                r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, verify=verify_arg)
            except requests.exceptions.SSLError:
                # Fallback to insecure if verification failed
                if not INSECURE:
                    log(f"[WARN] SSL Verify failed for {url}. Retrying with verify=False.")
                    # suppress warnings for cleaner output
                    import urllib3
                    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                    r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, verify=False)
                else:
                    raise

            log(f"[DEBUG] Attempt {attempt + 1} Status: {r.status_code}")
            r.raise_for_status()
            return r.text, r.url
        except Exception as e:
            last_exc = e
            log(f"[ERROR] Fetch failed (attempt {attempt + 1}): {e}")
            # backoff
            time.sleep(1 + attempt)
    log(f"[ERROR] All fetch attempts failed for: {url}  (last error: {last_exc})")
    return None, None

def extract_image_from_html(html: str, base_url: str, selector: str) -> Optional[str]:
    """Return resolved image URL found using the provided CSS selector."""
    log(f"[DEBUG] Parsing HTML from: {base_url} with selector: '{selector}'")
    soup = BeautifulSoup(html, "html.parser")

    img_tag = soup.select_one(selector)
    if not img_tag:
        log(f"[WARN] No element found matching selector: '{selector}'")
        return None

    src = img_tag.get("src") or img_tag.get("data-src") or ""
    log(f"[DEBUG] Raw image src: {repr(src)}")
    src = src.strip()
    if not src:
        log("[WARN] <img> src is empty")
        return None
    if src.startswith("data:"):
        log("[WARN] Skipping data URI image")
        return None

    resolved = urljoin(base_url, src)
    log(f"[DEBUG] Resolved image URL: {resolved}")
    return resolved

def find_pagecol(fieldnames: list[str]) -> Optional[str]:
    """Find the best candidate column name for PageUrl (case-insensitive)."""
    for c in fieldnames:
        if c and c.lower() == PAGEURL_COLUMN_NAME.lower():
            return c
    for c in fieldnames:
        if c and 'page' in c.lower() and 'url' in c.lower():
            return c
    for c in fieldnames:
        if c and 'url' in c.lower():
            return c
    return None

def process_row(page_url: str, selector: str) -> str:
    """Process a single page URL and return the resolved image URL or empty string."""
    log(f"\n=== Processing PageUrl: {page_url} ===")
    if not page_url:
        log("[WARN] Empty PageUrl")
        return ""
    html, final_url = fetch_html(page_url)
    if not html:
        log("[ERROR] No HTML returned for page")
        log_missing(f"URL_FAIL: {page_url}")
        return ""
    img = extract_image_from_html(html, final_url or page_url, selector)
    log(f"[DEBUG] Final extracted image URL: {img}")
    
    if not img:
        log_missing(f"IMG_MISSING: {page_url}")

    time.sleep(SLEEP_BETWEEN_REQUESTS)
    return img or ""

def main() -> None:
    global VERBOSE, VERIFY_BUNDLE, INSECURE
    parser = argparse.ArgumentParser(description="Populate ImageURL by scraping pages.")
    parser.add_argument("-n", "--limit", type=int, default=10,
                        help="Number of rows to process. Default 10. Use 0 for all rows.")
    parser.add_argument("--input", default=INPUT_CSV, help=f"Input CSV filename (default: {INPUT_CSV})")
    parser.add_argument("--output", default=None, help="Output CSV filename. If not set, defaults to [InputFilename]_with_images.csv")
    parser.add_argument("--selector", default=DEFAULT_IMAGE_SELECTOR, help=f"CSS selector to find the image (default: '{DEFAULT_IMAGE_SELECTOR}')")
    parser.add_argument("--workers", type=int, default=NUM_WORKERS, help="Number of concurrent workers.")
    parser.add_argument("--verbose", action="store_true", help="Enable detailed logging for debugging.")
    parser.add_argument("--insecure", action="store_true", help="Disable SSL verification (unsafe; for debugging).")
    parser.add_argument("--ca-bundle", default=None, help="Path to a custom CA bundle (PEM) to use for verification.")
    parser.add_argument("--error-log", default="fetch_errors.log", help="File to log missing images/URLs to (default: fetch_errors.log)")
    args = parser.parse_args()

    global VERBOSE, INSECURE, VERIFY_BUNDLE, ERROR_LOG_FILE

    VERBOSE = args.verbose
    INSECURE = args.insecure
    ERROR_LOG_FILE = args.error_log
    if args.ca_bundle:
        VERIFY_BUNDLE = args.ca_bundle
    else:
        # try to import certifi and use its bundle
        try:
            import certifi
            VERIFY_BUNDLE = certifi.where()
            log(f"[DEBUG] Using certifi CA bundle: {VERIFY_BUNDLE}")
        except Exception:
            VERIFY_BUNDLE = None
            if not INSECURE:
                print("[WARN] 'certifi' not available. Install with: pip install certifi")
                print("You can also pass --ca-bundle /path/to/ca-bundle.pem or use --insecure to skip verification.")
                # continue — requests will use system defaults (which on mac sometimes fail)
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"[ERROR] Input file not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    # Read CSV and detect PageUrl column case-insensitively
    with open(input_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        if not fieldnames:
            print("[ERROR] Input CSV has no header.", file=sys.stderr)
            sys.exit(1)
        page_col = find_pagecol(fieldnames)
        if not page_col:
            print(f"[ERROR] Could not find a PageUrl-like column in {args.input}.", file=sys.stderr)
            print("Existing columns:", fieldnames, file=sys.stderr)
            sys.exit(1)
        rows = [r for r in reader]

    total_rows_available = len(rows)
    limit = args.limit
    if limit < 0:
        limit = 0
    if limit == 0:
        to_process = rows[:]  # all
    else:
        to_process = rows[:min(limit, total_rows_available)]

    if not to_process:
        print("[INFO] No rows to process (limit resulted in 0 rows). Exiting.")
        sys.exit(0)

    print(f"[INFO] Processing {len(to_process)} row(s) (of {total_rows_available} available). Insecure={INSECURE}")

    # Process concurrently and collect image URLs
    results: list[str] = ["" for _ in range(len(to_process))]
    completed_count = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        future_to_index = {}
        for idx, row in enumerate(to_process):
            url = (row.get(page_col) or "").strip()
            future = ex.submit(process_row, url, args.selector)
            future_to_index[future] = (idx, url)

        for fut in as_completed(future_to_index):
            idx, url = future_to_index[fut]
            try:
                imgurl = fut.result()
            except Exception as e:
                imgurl = ""
                log(f"[ERROR] Exception while processing {url}: {e}")
            results[idx] = imgurl
            completed_count += 1
            found_text = "yes" if imgurl else "no"
            print(f"[PROG] Completed [{completed_count}/{len(to_process)}] idx={idx+1} -> image found: {found_text} (PageUrl: {url})")

    # Attach image URLs to corresponding rows (only for the processed subset)
    for i, img in enumerate(results):
        to_process[i][IMAGE_COLUMN_NAME] = img

    # Filter: only rows with a non-empty ImageURL
    rows_with_images = [r for r in to_process if r.get(IMAGE_COLUMN_NAME)]

    if not rows_with_images:
        print("[INFO] No images were found for the processed rows. No output file will be written.")
        sys.exit(0)

    # Prepare output headers (preserve original headers, add ImageURL if missing)
    out_fieldnames = list(fieldnames)
    if IMAGE_COLUMN_NAME not in out_fieldnames:
        out_fieldnames.append(IMAGE_COLUMN_NAME)

    if args.output:
        output_path = Path(args.output)
    else:
        # Auto-generate output filename based on input
        # e.g. sample.csv -> sample_with_images.csv
        stem = input_path.stem
        suffix = input_path.suffix
        output_path = input_path.with_name(f"{stem}_with_images{suffix}")

    output_path = Path(output_path)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_fieldnames)
        writer.writeheader()
        for row in rows_with_images:
            if IMAGE_COLUMN_NAME not in row:
                row[IMAGE_COLUMN_NAME] = ""
            writer.writerow(row)

    print(f"[INFO] Wrote {len(rows_with_images)} row(s) with images to: {output_path}")

if __name__ == "__main__":
    main()
