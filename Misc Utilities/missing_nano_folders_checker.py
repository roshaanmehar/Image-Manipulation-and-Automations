#!/usr/bin/env python3
r"""
Hard-coded version with robust diagnostics.

- Root folder scanned: C:\Roshaan\Code\Nano Banana Gen\output_images
- CSV filename (same folder as this script): output_products_with_categories.csv
- Output (same folder as this script): missing_items.csv
"""

import csv
import os
import sys
from typing import Dict, List, Set, Tuple

# =========================
# 🔧 HARDCODED SETTINGS
# =========================
ROOT_FOLDER = r"C:\Roshaan\Code\nanoBananGen\output_images"
CSV_BASENAME = "output_products_with_categories.csv"  # in the same folder as this script
OUT_BASENAME = "missing_items.csv"                    # written next to this script

# Column header variants we’ll accept for the item code and barcode
CODE_HEADER_CANDIDATES = [
    "Code (CM)", "CODE (CM)", "Code", "CM Code", "Code(CM)", "Code  (CM)"
]
BARCODE_HEADER_CANDIDATES = [
    "Unit Barcode", "UNIT BARCODE", "Barcode", "BARCODE", "Outer Box Barcode"
]

def script_dir() -> str:
    try:
        return os.path.dirname(os.path.abspath(__file__))
    except NameError:
        return os.getcwd()

def normalise(s: str) -> str:
    return " ".join((s or "").strip().lower().split())

def find_header(name_candidates: List[str], headers: List[str]) -> str:
    """Find a header from candidates (case/space tolerant)."""
    header_norm = {normalise(h): h for h in headers}
    for cand in name_candidates:
        if normalise(cand) in header_norm:
            return header_norm[normalise(cand)]
    raise KeyError(f"Could not find any of {name_candidates} in CSV headers: {headers}")

def read_csv_rows(csv_path: str) -> Tuple[List[Dict[str, str]], str, str]:
    """Read CSV to a list of dicts; return rows and detected header names for code and barcode."""
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found at: {csv_path}")
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        # Your file is comma-separated; avoid sniffing weirdness
        reader = csv.DictReader(f, delimiter=",")
        headers = reader.fieldnames or []
        if not headers:
            raise ValueError("CSV has no headers. Check the file format/encoding.")
        print(f"[INFO] CSV headers detected: {headers}")
        rows = list(reader)
        if not rows:
            raise ValueError("CSV appears to be empty or has only headers.")
    # Find headers we care about
    code_header = find_header(CODE_HEADER_CANDIDATES, headers)
    try:
        barcode_header = find_header(BARCODE_HEADER_CANDIDATES, headers)
    except KeyError:
        barcode_header = ""
        print("[WARN] No barcode-like header found; continuing without barcode checks.")
    print(f"[INFO] Using code header: '{code_header}'" + (f", barcode header: '{barcode_header}'" if barcode_header else ""))
    return rows, code_header, barcode_header

def extract_csv_codes(rows: List[Dict[str, str]], code_header: str) -> Set[str]:
    codes = set()
    missing_count = 0
    for r in rows:
        val = (r.get(code_header) or "").strip().upper()
        if val:
            codes.add(val)
        else:
            missing_count += 1
    if missing_count:
        print(f"[WARN] {missing_count} rows had an empty '{code_header}' value and were skipped.")
    return codes

def parse_folder_itemcode(folder_name: str) -> Tuple[str, str]:
    """
    Expect 'ITEMCODE - BARCODE'. Return (itemcode_upper, barcode_raw).
    If malformed, return ("", "").
    """
    name = folder_name.strip()
    # Accept either " - " or "-" with flexible spacing.
    parts = [p.strip() for p in name.split("-")]
    if len(parts) < 2:
        return "", ""
    itemcode = parts[0].strip().upper()
    barcode = "-".join(parts[1:]).strip()
    if not itemcode:
        return "", ""
    return itemcode, barcode

def scan_folder_itemcodes(root: str) -> Tuple[Set[str], List[str], Dict[str, str]]:
    itemcodes: Set[str] = set()
    malformed: List[str] = []
    code_to_barcode: Dict[str, str] = {}

    if not os.path.exists(root):
        raise FileNotFoundError(f"Root folder not found: {root}")
    entries = os.listdir(root)
    if not entries:
        print(f"[WARN] Root folder is empty: {root}")

    for e in entries:
        full = os.path.join(root, e)
        if not os.path.isdir(full):
            continue
        itemcode, barcode = parse_folder_itemcode(e)
        if not itemcode:
            malformed.append(e)
            continue
        itemcodes.add(itemcode)
        if barcode:
            code_to_barcode[itemcode] = barcode

    return itemcodes, malformed, code_to_barcode

def write_missing_csv(rows: List[Dict[str, str]], code_header: str, missing_codes: Set[str], out_path: str) -> None:
    headers = list(rows[0].keys())
    to_write = [r for r in rows if (r.get(code_header) or "").strip().upper() in missing_codes]
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for r in to_write:
            writer.writerow(r)

def main():
    base = script_dir()
    csv_path = os.path.join(base, CSV_BASENAME)
    out_path = os.path.join(base, OUT_BASENAME)

    print("=== Startup Checks ===")
    print(f"[INFO] Script directory: {base}")
    print(f"[INFO] Expecting CSV here: {csv_path}")
    print(f"[INFO] Scanning root folder: {ROOT_FOLDER}")
    if not os.path.exists(csv_path):
        print("[ERROR] CSV file not found at the expected location.")
        print(f"        Ensure the CSV is named '{CSV_BASENAME}' and sits next to this script.")
        sys.exit(1)
    if not os.path.exists(ROOT_FOLDER):
        print("[ERROR] ROOT_FOLDER path does not exist.")
        sys.exit(1)

    try:
        rows, code_header, barcode_header = read_csv_rows(csv_path)
        csv_codes = extract_csv_codes(rows, code_header)
        folder_codes, malformed, code_to_barcode = scan_folder_itemcodes(ROOT_FOLDER)
    except Exception as e:
        print(f"[FATAL] {type(e).__name__}: {e}")
        sys.exit(1)

    found = csv_codes & folder_codes
    missing = csv_codes - folder_codes
    extra = folder_codes - csv_codes

    print("\n=== Comparison Summary ===")
    print(f"Total unique codes in CSV: {len(csv_codes)}")
    print(f"Total item subfolders:     {len(folder_codes)}")
    print(f"Found in both:             {len(found)}")
    print(f"Missing (CSV → no folder): {len(missing)}")
    print(f"Extras (folder → no CSV):  {len(extra)}")
    if malformed:
        print(f"[WARN] Malformed subfolder names (ignored): {len(malformed)}")
        for example in malformed[:10]:
            print(f"  - {example}")

    write_missing_csv(rows, code_header, missing, out_path)
    print(f"\n[OK] Wrote missing items to: {out_path}")

    if missing:
        print("\nFirst few missing codes:")
        for c in list(sorted(missing))[:10]:
            print(f"  - {c}")
    if extra:
        print("\nFirst few extra folder codes not in CSV:")
        for c in list(sorted(extra))[:10]:
            print(f"  - {c}")

if __name__ == "__main__":
    main()
