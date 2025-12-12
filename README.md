# 📰 News Image URL Extractor

A tool to extract image URLs from news detail pages. It reads a CSV file containing page URLs, scrapes the images, and outputs a new CSV with the `ImageURL` column populated.

---

## 🚀 Features

* **Concurrency**: Uses multiple threads for fast scraping.
* **Smart Retry**: Automatically retries failed requests with exponential backoff.
* **SSL Fallback**: Safely handles SSL verification errors by retrying with verification disabled if needed.
* **Error Logging**: Logs missing images and failed URLs to `fetch_errors.log`.
* **Configurable**: Fully customizable via command-line arguments.

---

## 📁 Requirements

Install dependencies using pip:

```bash
pip install requests beautifulsoup4 certifi urllib3
```

Python version: **3.8+**

---

## ▶️ How to Run

### Basic Usage (using defaults)

By default, the script looks for `sample.csv` and outputs to `sample_with_images.csv`.

```bash
python3 fetch-image.py
```

### using a Custom Input File

To process your specific data export:

```bash
python3 fetch-image.py --input MOJ_News_CleanExport.csv --output results.csv
```

### Other Common Commands

**Process ALL rows:**
```bash
python3 fetch-image.py -n 0
```

**Process 50 rows:**
```bash
python3 fetch-image.py -n 50
```

**Enable Verbose Logging (Debugging):**
```bash
python3 fetch-image.py --verbose
```

---

## 📄 Input File Format

The script expects a CSV file with at least a **PageUrl** column.
The default input file is `sample.csv`.

**Required Columns:**
* `PageUrl`: The full URL of the news page.
* `ImageURL`: (Optional) Will be filled by the script.

---

## ⚙️ Configuration Options

| Flag | Description | Default |
| :--- | :--- | :--- |
| `-n`, `--limit` | Number of rows to process (`0` for all). | `10` |
| `--input` | Input CSV filename. | `sample.csv` |
| `--output` | Output CSV filename. | `[Input]_with_images.csv` |
| `--workers` | Number of concurrent worker threads. | `8` |
| `--error-log`| File to log errors to. | `fetch_errors.log` |
| `--verbose` | Enable detailed debug logging. | `False` |
| `--insecure` | Disable SSL verification entirely. | `False` |

---

## ❗ Git & Version Control

* **`sample.csv`** is included in the repo as a template.
* **`*.csv`** files are ignored by git (except `sample.csv`) to protect your data.
* **`*.log`** files are ignored.
