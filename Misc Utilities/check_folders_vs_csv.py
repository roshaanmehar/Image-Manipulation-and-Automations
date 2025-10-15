#!/usr/bin/env python3
import csv
import os
import re
import sys
from pathlib import Path
from collections import defaultdict

# ---------- helpers ----------
def norm_code(s: str) -> str:
    """Uppercase, remove spaces. Handles None safely."""
    return re.sub(r"\s+", "", (s or "")).upper()

def digits_only(s: str) -> str:
    """Keep only 0-9 (barcodes sometimes arrive with spaces or punctuation)."""
    return "".join(ch for ch in (s or "") if ch.isdigit())

def find_col(header: dict, wanted: str):
    """Find a column by case-insensitive substring match (tolerant of spaces, punctuation)."""
    wanted_n = re.sub(r"[^a-z0-9]+", "", wanted.lower())
    for k in header.keys():
        k_n = re.sub(r"[^a-z0-9]+", "", k.lower())
        if k_n == wanted_n:
            return k
    # soft fallback: contains
    for k in header.keys():
        if wanted_n in re.sub(r"[^a-z0-9]+", "", k.lower()):
            return k
    raise KeyError(f"Could not find column named like: {wanted!r}")

def parse_folder_name(name: str):
    """
    Expected: ITEMCODE ' - ' BARCODE.
    Accepts hyphen or en dash, with flexible spaces.
    Returns (itemcode, barcode) or None if it doesn't look right.
    """
    m = re.match(r"^\s*(.+?)\s*[-–]\s*([0-9\s]+)\s*$", name)
    if not m:
        return None
    itemcode = norm_code(m.group(1))
    barcode = digits_only(m.group(2))
    if not itemcode or not barcode:
        return None
    return itemcode, barcode

# ---------- main ----------
def main(root_dir: Path, csv_path: Path):
    # 1) parse folders
    folder_pairs = []  # list of (folder_path, itemcode, barcode)
    for entry in root_dir.iterdir():
        if entry.is_dir():
            parsed = parse_folder_name(entry.name)
            if parsed:
                itemcode, barcode = parsed
                folder_pairs.append((entry, itemcode, barcode))

    # Make quick lookups
    barcodes_in_folders = set(b for _, _, b in folder_pairs)

    # 2) read CSV
    # Use utf-8-sig to swallow any BOM; keep commas as-is (currency/percentages fine).
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        if not rows:
            print("CSV appears empty.")
            return

    # find columns robustly
    hdr = reader.fieldnames or []
    header_map = {h: h for h in hdr}
    col_code = find_col(header_map, "Code (CM)")
    col_unit_barcode = find_col(header_map, "Unit Barcode")
    col_name = find_col(header_map, "Name / Description") if any(
        "name" in (h.lower()) for h in hdr
    ) else None

    # build lookup by barcode
    by_barcode = {}
    for r in rows:
        bc = digits_only(r.get(col_unit_barcode, ""))
        if bc:
            by_barcode[bc] = r

    # 3) compare
    discrepancies = []  # rows for output
    not_in_csv = []     # barcodes from folders that are unknown to CSV

    for folder_path, f_code, f_bar in folder_pairs:
        csv_row = by_barcode.get(f_bar)
        if not csv_row:
            not_in_csv.append({
                "Folder": str(folder_path),
                "Folder ItemCode": f_code,
                "Folder Barcode": f_bar,
                "Issue": "Barcode not found in CSV"
            })
            continue

        csv_code = norm_code(csv_row.get(col_code, ""))
        if csv_code != f_code:
            discrepancies.append({
                "Folder": str(folder_path),
                "Folder ItemCode": f_code,
                "Folder Barcode": f_bar,
                "CSV Code (CM)": csv_code,
                "CSV Unit Barcode": digits_only(csv_row.get(col_unit_barcode, "")),
                "Issue": "ItemCode mismatch for this barcode"
            })

    # 4) barcodes present in CSV but with no folder
    csv_barcodes_not_in_folders = []
    for bc, row in by_barcode.items():
        if bc not in barcodes_in_folders:
            csv_barcodes_not_in_folders.append({
                "CSV Unit Barcode": bc,
                "CSV Code (CM)": norm_code(row.get(col_code, "")),
                "Name / Description": (row.get(col_name, "") if col_name else "")
            })

    # 5) write reports
    out_dir = root_dir / "_reports"
    out_dir.mkdir(exist_ok=True)

    def write_csv(p: Path, rows: list, fieldnames: list):
        with p.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for r in rows:
                w.writerow(r)

    if discrepancies:
        write_csv(
            out_dir / "discrepancies.csv",
            discrepancies,
            ["Folder", "Folder ItemCode", "Folder Barcode", "CSV Code (CM)", "CSV Unit Barcode", "Issue"]
        )

    if not_in_csv:
        write_csv(
            out_dir / "folders_unknown_to_csv.csv",
            not_in_csv,
            ["Folder", "Folder ItemCode", "Folder Barcode", "Issue"]
        )

    write_csv(
        out_dir / "csv_barcodes_missing_folders.csv",
        csv_barcodes_not_in_folders,
        ["CSV Unit Barcode", "CSV Code (CM)", "Name / Description"]
    )

    # 6) console summary
    print(f"Folders parsed: {len(folder_pairs)}")
    print(f"Discrepancies:  {len(discrepancies)} (see _reports/discrepancies.csv)")
    print(f"Folders unknown to CSV: {len(not_in_csv)} (see _reports/folders_unknown_to_csv.csv)")
    print(f"CSV barcodes with no folder: {len(csv_barcodes_not_in_folders)} (see _reports/csv_barcodes_missing_folders.csv)")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python check_folders_vs_csv.py <folder_root> <csv_file>")
        sys.exit(1)
    main(Path(sys.argv[1]), Path(sys.argv[2]))
