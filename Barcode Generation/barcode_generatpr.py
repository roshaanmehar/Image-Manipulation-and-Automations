#!/usr/bin/env python3
import argparse
import csv
import os
import re
import time
from io import BytesIO
from urllib.parse import quote

import requests

# ---- Config you can tweak if needed ----
MASTER_DIR = "barcodes_master"
BY_CATEGORY_DIR = "by_category"
BARCODE_API_BASE = "https://barcodeapi.org/api"   # auto will pick EAN-13
API_SYMBOLOGY = "auto"                             # or "ean13"
REQUEST_TIMEOUT = 20
PAUSE_SECONDS = 0.15                               # gentle pacing
# ----------------------------------------

def safe_category_dir(name: str) -> str:
    # keep human-readable names, but prevent filesystem issues
    # replace slashes/backslashes with hyphens; trim spaces
    cleaned = name.strip().replace("/", "-").replace("\\", "-")
    # avoid weird whitespace
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned or "Uncategorised"

def extract_barcode(value: str) -> str:
    # Strip surrounding spaces; keep digits only to avoid stray characters
    # (preserves leading zeros if any)
    digits = re.sub(r"[^\d]", "", value or "")
    return digits

def fetch_barcode_png(barcode: str) -> bytes:
    # barcodeapi supports /api/<symbology>/<data>
    url = f"{BARCODE_API_BASE}/{API_SYMBOLOGY}/{quote(barcode)}"
    r = requests.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.content

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def write_png(path: str, content: bytes):
    with open(path, "wb") as f:
        f.write(content)

def main(csv_path: str, master_dir: str, by_category_dir: str):
    ensure_dir(master_dir)
    ensure_dir(by_category_dir)

    # Read CSV carefully: there are embedded spaces and blank columns
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        # normalise header keys by stripping whitespace
        reader.fieldnames = [h.strip() for h in reader.fieldnames]

        # Figure out the exact column names
        # Required: "Unit Barcode" and "Category"
        # Be tolerant of accidental spacing/case
        def find_col(wanted):
            for col in reader.fieldnames:
                if col.strip().lower() == wanted.strip().lower():
                    return col
            raise KeyError(f"CSV missing required column: {wanted}")

        unit_barcode_col = find_col("Unit Barcode")
        category_col = find_col("Category")

        seen_barcodes = set()
        categories_set = set()
        total_rows = 0
        saved = 0
        skipped = 0
        errors = 0

        for row in reader:
            total_rows += 1
            raw_barcode = row.get(unit_barcode_col, "")
            category_raw = row.get(category_col, "")

            barcode = extract_barcode(raw_barcode)
            category = safe_category_dir(category_raw)

            # Track distinct categories
            if category:
                categories_set.add(category)

            # sanity checks
            if not barcode:
                skipped += 1
                print(f"[skip] row {total_rows}: empty or invalid Unit Barcode: {raw_barcode!r}")
                continue

            if barcode in seen_barcodes:
                # Avoid re-downloading duplicates, but still copy into category if needed
                master_path = os.path.join(master_dir, f"{barcode}.png")
                cat_dir = os.path.join(by_category_dir, category)
                ensure_dir(cat_dir)
                cat_path = os.path.join(cat_dir, f"{barcode}.png")

                if os.path.exists(master_path) and not os.path.exists(cat_path):
                    try:
                        # Hard link if possible; fallback to copy bytes from master
                        try:
                            os.link(master_path, cat_path)
                        except Exception:
                            with open(master_path, "rb") as src:
                                write_png(cat_path, src.read())
                        saved += 1
                    except Exception as e:
                        errors += 1
                        print(f"[error] linking/copying duplicate barcode {barcode} to category '{category}': {e}")
                else:
                    skipped += 1
                continue

            # Fetch and write
            try:
                content = fetch_barcode_png(barcode)
                # Write to master
                master_path = os.path.join(master_dir, f"{barcode}.png")
                write_png(master_path, content)

                # Write to category
                cat_dir = os.path.join(by_category_dir, category)
                ensure_dir(cat_dir)
                cat_path = os.path.join(cat_dir, f"{barcode}.png")

                # Try hard link to save space; fallback to writing bytes
                try:
                    os.link(master_path, cat_path)
                except Exception:
                    write_png(cat_path, content)

                seen_barcodes.add(barcode)
                saved += 1
                print(f"[ok] {barcode} -> {master_path} and {cat_path}")

                time.sleep(PAUSE_SECONDS)

            except requests.HTTPError as e:
                errors += 1
                print(f"[error] HTTP {e.response.status_code} for barcode {barcode}: {e}")
            except Exception as e:
                errors += 1
                print(f"[error] barcode {barcode}: {e}")

        print("\n---- Summary ----")
        print(f"Rows processed : {total_rows}")
        print(f"Saved images   : {saved}")
        print(f"Skipped rows   : {skipped} (missing/duplicate)")
        print(f"Errors         : {errors}")
        print(f"Unique categories found: {len(categories_set)}")
        if len(categories_set) != 9:
            # Not fatal, just informative
            print("Note: The dataset does not contain exactly 9 unique categories. "
                  "Folders were created for whatever was present.")

        # Create any missing category folders up to 9? We won’t guess—only create what exists in data.

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate barcode PNGs from a CSV via barcodeapi.org")
    parser.add_argument("--csv", required=True, help="Path to the CSV file")
    parser.add_argument("--master-dir", default=MASTER_DIR, help="Master barcode folder name")
    parser.add_argument("--by-category-dir", default=BY_CATEGORY_DIR, help="By-category root folder name")
    args = parser.parse_args()

    main(args.csv, args.master_dir, args.by_category_dir)
