#!/usr/bin/env python3
"""
download-images.py

Downloads images listed in a CSV file (in the 'ImageURL' column) to a local directory.
Updates the CSV with the local filename and relative path, suitable for Drupal migration.

Usage:
    python3 download-images.py --input sample_with_images.csv --output final_migration_data.csv
"""
import argparse
import csv
import hashlib
import mimetypes
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional, Dict, Any
from urllib.parse import urlparse

import requests

# Defaults
INPUT_CSV = "sample_with_images.csv"

IMAGE_DIR = "downloaded_images"
ID_COLUMN = "ID"
IMAGE_URL_COLUMN = "ImageURL"
NUM_WORKERS = 8
REQUEST_TIMEOUT = 10
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; MOJImageDownloader/1.0)"
}

def get_extension_from_url(url: str, content_type: Optional[str]) -> str:
    """Guess extension from URL or Content-Type header."""
    parsed = urlparse(url)
    path = parsed.path
    ext = os.path.splitext(path)[1]
    if ext:
        return ext.lower()
    
    # Try guessing from text/html or image/jpeg etc
    if content_type:
        guessed = mimetypes.guess_extension(content_type.split(";")[0].strip())
        if guessed:
            return guessed
    return ".jpg" # Fallback

def download_image(url: str, record_id: str, output_dir: Path) -> Dict[str, str]:
    """
    Downloads image from url.
    Returns dictionary with keys for the new CSV columns: 'ImageFileName', 'ImageFilePath'.
    Returns empty values on failure.
    """
    if not url:
        return {"ImageFileName": "", "ImageFilePath": ""}

    try:
        # Request the image
        r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, verify=False) # Skip SSL verify for legacy sites
        r.raise_for_status()
        
        # Determine filename
        # Naming convention: news_<ID>.<ext>
        ext = get_extension_from_url(url, r.headers.get("Content-Type"))
        if not ext.startswith("."):
            ext = "." + ext
            
        # Sanitize ID for filename
        safe_id = "".join(c for c in record_id if c.isalnum() or c in ('-', '_')) if record_id else "unknown"
        if not safe_id or safe_id == "unknown":
            # fallback to hash of url if no ID
            hash_object = hashlib.md5(url.encode())
            safe_id = hash_object.hexdigest()[:10]

        filename = f"news_{safe_id}{ext}"
        filepath = output_dir / filename
        
        # Write to disk
        with open(filepath, "wb") as f:
            f.write(r.content)
            
        # Return relative path logic
        # User requested: "name of the file and relative path in the folder"
        # Relative path e.g.: downloaded_images/news_123.jpg
        relative_path = os.path.join(output_dir.name, filename)
        
        return {
            "ImageFileName": filename,
            "ImageFilePath": relative_path
        }

    except Exception as e:
        print(f"[ERROR] Failed to download {url}: {e}")
        return {"ImageFileName": "", "ImageFilePath": ""}

def process_row(row: Dict[str, Any], output_dir: Path) -> Dict[str, Any]:
    url = row.get(IMAGE_URL_COLUMN, "").strip()
    record_id = row.get(ID_COLUMN, "").strip()
    
    if not url:
        # No image to download
        row["ImageFileName"] = ""
        row["ImageFilePath"] = ""
        return row
        
    result = download_image(url, record_id, output_dir)
    row.update(result)
    return row

def main():
    parser = argparse.ArgumentParser(description="Download images from CSV and update file paths.")
    parser.add_argument("--input", default=INPUT_CSV, help=f"Input CSV file (default: {INPUT_CSV})")

    parser.add_argument("--output", default=None, help="Output CSV file (default: [Input]_downloaded.csv)")
    parser.add_argument("--folder", default=IMAGE_DIR, help=f"Directory to save images (default: {IMAGE_DIR})")
    parser.add_argument("--workers", type=int, default=NUM_WORKERS, help="Concurrent downloads")
    parser.add_argument("-n", "--limit", type=int, default=0, help="Number of rows to process (0 for all)")
    
    args = parser.parse_args()
    
    # Setup directories
    out_dir = Path(args.folder)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    # Verify input
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"[ERROR] Input file {input_path} not found.")
        sys.exit(1)
        
    print(f"[INFO] Reading from {input_path}...")
    
    rows = []
    with open(input_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    if args.limit > 0:
        rows = rows[:args.limit]
        print(f"[INFO] Limited to first {args.limit} rows.")
        
    if IMAGE_URL_COLUMN not in fieldnames:
        print(f"[ERROR] Column '{IMAGE_URL_COLUMN}' not found in CSV.")
        sys.exit(1)
        
    # Prepare output fields
    out_fieldnames = list(fieldnames)
    if "ImageFileName" not in out_fieldnames:
        out_fieldnames.append("ImageFileName")
    if "ImageFilePath" not in out_fieldnames:
        out_fieldnames.append("ImageFilePath")
    
    print(f"[INFO] Starting download of {len(rows)} records to '{out_dir}/'...")
    
    # Disable SSL warnings globally for this run
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    processed_rows = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_to_row = {executor.submit(process_row, row.copy(), out_dir): row for row in rows}
        
        count = 0
        total = len(rows)
        
        for future in as_completed(future_to_row):
            count += 1
            try:
                updated_row = future.result()
                processed_rows.append(updated_row)
                
                # Progress logging
                img_path = updated_row.get("ImageFileName", "")
                status = "Downloaded" if img_path else "Skipped/Failed"
                if count % 10 == 0 or count == total:
                    print(f"[PROG] {count}/{total} - Last status: {status}")
                    
            except Exception as e:
                print(f"[ERROR] Exception processing row: {e}")
                # Append original row with empty fields if crash
                original = future_to_row[future]
                original["ImageFileName"] = ""
                original["ImageFilePath"] = ""
                processed_rows.append(original)

    # Sort back to original order if needed? ThreadPool shuffles them.
    # To keep order, we might map by ID or just index.
    # Simpler: just write them out. Order usually doesn't matter for migration as long as ID is there.
    # But for niceness, let's try to sort by ID if possible, or just leave as is.
    
    # Determine output path
    if args.output:
        output_path = Path(args.output)
    else:
        # e.g. Input.csv -> Input_downloaded.csv
        stem = input_path.stem
        suffix = input_path.suffix
        output_path = input_path.with_name(f"{stem}_downloaded{suffix}")

    print(f"[INFO] Writing results to {output_path}...")
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=out_fieldnames)
        writer.writeheader()
        writer.writerows(processed_rows)
        
    print("[INFO] Done.")

if __name__ == "__main__":
    main()
