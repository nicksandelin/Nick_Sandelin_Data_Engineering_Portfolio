
"""
Created by Nick Sandelin 3/2/2026

etl.py — Sales flat-file ETL for Dolt / MySQL (Operations.sales)

Workflow - 
--------
1. Discover all sales*.txt files in nicks/Documents/pyETL (excludes archived/).
2. Parse each file as CSV, validate rows, stamp processing_date.
3. Bulk-upsert valid rows into Operations.sales via INSERT IGNORE.
4. Move successfully processed files to ARCHIVE_DIR.
5. Print a per-file summary and an overall run summary.

Expected file columns (order does not matter — header row is required):
    sale_id, customer_name, product, quantity, unit_price,
    total_amount, sale_date, region, salesperson
"""

import csv
import glob
import logging
import os
import shutil
import sys
from datetime import datetime
from decimal import Decimal, InvalidOperation

import mysql.connector
from mysql.connector import Error as MySQLError

import config

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(config.SOURCE_DIR, "etl.log"), encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
REQUIRED_COLUMNS = {
    "sale_id", "customer_name", "product", "quantity",
    "unit_price", "total_amount", "sale_date", "region", "salesperson",
}

INSERT_SQL = """
    INSERT IGNORE INTO sales
        (sale_id, customer_name, product, quantity, unit_price,
         total_amount, sale_date, region, salesperson,
         processing_date, source_file)
    VALUES
        (%(sale_id)s, %(customer_name)s, %(product)s, %(quantity)s,
         %(unit_price)s, %(total_amount)s, %(sale_date)s, %(region)s,
         %(salesperson)s, %(processing_date)s, %(source_file)s)
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_connection():
    """Return an open mysql.connector connection to Dolt."""
    conn = mysql.connector.connect(**config.DB_CONFIG)
    return conn


def discover_files():
    """Return sorted list of absolute paths matching FILE_PATTERN in SOURCE_DIR,
    explicitly excluding files already inside the archive subdirectory."""
    pattern = os.path.join(config.SOURCE_DIR, config.FILE_PATTERN)
    all_matches = glob.glob(pattern)
    archive_prefix = os.path.normcase(os.path.realpath(config.ARCHIVE_DIR))
    result = []
    for path in sorted(all_matches):
        if not os.path.normcase(os.path.realpath(path)).startswith(archive_prefix):
            result.append(path)
    return result


def parse_decimal(value: str, field: str) -> Decimal:
    try:
        return Decimal(value.strip().replace(",", ""))
    except InvalidOperation:
        raise ValueError(f"Invalid decimal for {field!r}: {value!r}")


def parse_int(value: str, field: str) -> int:
    try:
        return int(value.strip())
    except ValueError:
        raise ValueError(f"Invalid integer for {field!r}: {value!r}")


def parse_date(value: str, field: str) -> str:
    """Accept YYYY-MM-DD or MM/DD/YYYY and return YYYY-MM-DD."""
    value = value.strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    raise ValueError(f"Unrecognised date format for {field!r}: {value!r}")


def validate_row(raw: dict, source_file: str, processing_date: datetime) -> dict:
    """Parse and validate a single CSV row dict.  Returns a clean param dict."""
    row = {k.strip().lower(): v.strip() for k, v in raw.items()}

    missing = REQUIRED_COLUMNS - row.keys()
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    quantity     = parse_int(row["quantity"], "quantity")
    unit_price   = parse_decimal(row["unit_price"], "unit_price")
    total_amount = parse_decimal(row["total_amount"], "total_amount")

    # Soft-validate total_amount (warn but still load)
    expected = (Decimal(quantity) * unit_price).quantize(Decimal("0.01"))
    if total_amount != expected:
        log.warning(
            "total_amount mismatch for sale_id=%s: file=%s, expected=%s",
            row["sale_id"], total_amount, expected,
        )

    return {
        "sale_id":        row["sale_id"],
        "customer_name":  row["customer_name"],
        "product":        row["product"],
        "quantity":       quantity,
        "unit_price":     float(unit_price),
        "total_amount":   float(total_amount),
        "sale_date":      parse_date(row["sale_date"], "sale_date"),
        "region":         row["region"],
        "salesperson":    row["salesperson"],
        "processing_date": processing_date,
        "source_file":    os.path.basename(source_file),
    }


# ---------------------------------------------------------------------------
# Core ETL per file
# ---------------------------------------------------------------------------

def process_file(filepath: str, cursor, processing_date: datetime) -> dict:
    """Parse one file, insert rows, return stats dict."""
    filename = os.path.basename(filepath)
    stats = {"file": filename, "parsed": 0, "inserted": 0, "skipped": 0, "errors": 0}

    log.info("Processing: %s", filename)

    with open(filepath, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh, delimiter=config.DELIMITER)

        # Validate header presence
        if not reader.fieldnames:
            raise ValueError(f"{filename}: file is empty or has no header row")

        header_lower = {f.strip().lower() for f in reader.fieldnames}
        missing_cols = REQUIRED_COLUMNS - header_lower
        if missing_cols:
            raise ValueError(f"{filename}: missing required columns {missing_cols}")

        batch = []
        for line_num, raw_row in enumerate(reader, start=2):
            try:
                clean = validate_row(raw_row, filepath, processing_date)
                batch.append(clean)
                stats["parsed"] += 1
            except ValueError as exc:
                log.warning("  Line %d skipped — %s", line_num, exc)
                stats["errors"] += 1

        if batch:
            cursor.executemany(INSERT_SQL, batch)
            stats["inserted"] = cursor.rowcount   # rows actually inserted (IGNORE skips dupes)
            stats["skipped"]  = len(batch) - stats["inserted"]

    log.info(
        "  -> parsed=%d  inserted=%d  skipped(dupes)=%d  bad_rows=%d",
        stats["parsed"], stats["inserted"], stats["skipped"], stats["errors"],
    )
    return stats


def archive_file(filepath: str):
    """Move a processed file into ARCHIVE_DIR, appending a timestamp if needed."""
    os.makedirs(config.ARCHIVE_DIR, exist_ok=True)
    dest = os.path.join(config.ARCHIVE_DIR, os.path.basename(filepath))

    # Avoid collisions with already-archived copies
    if os.path.exists(dest):
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        name, ext = os.path.splitext(os.path.basename(filepath))
        dest = os.path.join(config.ARCHIVE_DIR, f"{name}_{ts}{ext}")

    shutil.move(filepath, dest)
    log.info("  Archived -> %s", os.path.basename(dest))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    log.info("=" * 60)
    log.info("Sales ETL run started")

    files = discover_files()
    if not files:
        log.info("No files matching '%s' found in %s — nothing to do.",
                 config.FILE_PATTERN, config.SOURCE_DIR)
        return

    log.info("Files found: %d", len(files))

    try:
        conn = get_connection()
    except MySQLError as exc:
        log.error("Cannot connect to database: %s", exc)
        sys.exit(1)

    processing_date = datetime.now()
    all_stats = []
    failed_files = []

    try:
        cursor = conn.cursor()

        for filepath in files:
            try:
                stats = process_file(filepath, cursor, processing_date)
                conn.commit()
                archive_file(filepath)
                all_stats.append(stats)
            except Exception as exc:
                conn.rollback()
                log.error("FAILED %s — %s", os.path.basename(filepath), exc)
                failed_files.append(os.path.basename(filepath))

        cursor.close()
    finally:
        conn.close()

    # Summary
    log.info("-" * 60)
    log.info("Run summary")
    total_parsed   = sum(s["parsed"]   for s in all_stats)
    total_inserted = sum(s["inserted"] for s in all_stats)
    total_skipped  = sum(s["skipped"]  for s in all_stats)
    total_errors   = sum(s["errors"]   for s in all_stats)

    log.info("  Files processed : %d", len(all_stats))
    log.info("  Files failed    : %d  %s", len(failed_files),
             failed_files if failed_files else "")
    log.info("  Rows parsed     : %d", total_parsed)
    log.info("  Rows inserted   : %d", total_inserted)
    log.info("  Rows skipped    : %d  (duplicate sale_id)", total_skipped)
    log.info("  Bad rows        : %d  (validation errors)", total_errors)
    log.info("Sales ETL run complete")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
