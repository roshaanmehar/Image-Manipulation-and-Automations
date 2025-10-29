#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
Hard-coded discrepancy & image-count checker

- CSV (same folder as this script): output_all_categories.csv
- Root folder scanned: C:\Roshaan\Code\nanoBananGen\output_images
- Reports outdir (same folder as this script): .\reports\
"""

import csv
import os
import sys
from pathlib import Path
from collections import Counter
from typing import Dict, List, Set, Tuple

# ---------- Hard-coded paths ----------
ROOT_FOLDER = r"C:\Roshaan\Code\nanoBananGen\output_images"
CSV_BASENAME = "output_all_categories.csv"  # sits next to this script
OUTDIR_NAME = "reports"                     # created next to this script

# ---------- Image audit config ----------
MIN_IMAGES_PER_FOLDER = 4
IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp", ".avif", ".tif", ".tiff"}

# ---------- Header guesses (case/space tolerant) ----------
CODE_HEADER_CANDIDATES = [
    "Code (CM)", "CODE (CM)", "Code", "CM Code", "Code(CM)", "Code  (CM)"
]
BARCODE_HEADER_CANDIDATES = [
    "Unit Barcode", "UNIT BARCODE", "Barcode", "BARCODE", "Outer Box Barcode"
]

def normalise_header(s: str) -> str:
    return " ".join((s or "").strip().lower().split())

def find_header(candidates: List[str], headers: List[str]) -> str:
    idx = {normalise_header(h): h for h in headers}
    for c in candidates:
        k = normalise_header(c)
        if k in idx:
            return idx[k]
    raise KeyError(f"Could not find any of {candidates} in CSV headers {headers}")

def script_dir() -> Path:
    try:
        return Path(os.path.abspath(__file__)).parent
    except NameError:
        return Path.cwd()

def read_csv(csv_path: Path):
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=",")
        headers = reader.fieldnames or []
        if not headers:
            raise ValueError("CSV has no headers")
        rows = list(reader)
        if not rows:
            raise ValueError("CSV has no data rows")
    code_header = find_header(CODE_HEADER_CANDIDATES, headers)
    try:
        barcode_header = find_header(BARCODE_HEADER_CANDIDATES, headers)
    except KeyError:
        barcode_header = ""
        print("[WARN] No barcode-like header found; skipping barcode cross-checks.")
    return rows, code_header, barcode_header

def parse_folder(folder_name: str) -> Tuple[str, str]:
    """
    Accept 'ITEMCODE - BARCODE' (flexible spaces/hyphens).
    Returns (ITEMCODE_UPPER, BARCODE_RAW) or ("","") if malformed.
    """
    name = folder_name.strip()
    parts = [p.strip() for p in name.split("-")]
    if len(parts) < 2:
        return "", ""
    code = parts[0].strip().upper()
    bc = "-".join(parts[1:]).strip()
    if not code or not bc:
        return "", ""
    return code, bc

def count_images_in_folder(path: Path) -> int:
    if not path.exists() or not path.is_dir():
        return 0
    n = 0
    for entry in path.iterdir():
        if entry.is_file() and entry.suffix.lower() in IMAGE_EXTS:
            n += 1
    return n

def scan_root(root: Path):
    """
    Returns:
      code_to_barcode: ITEMCODE -> BARCODE
      malformed: list of bad folder names
      folder_code_counts: Counter of ITEMCODE (to spot dupes)
      image_counts: ITEMCODE -> image count (only for well-formed folders)
      folder_paths: ITEMCODE -> Path to that folder (last wins if duplicates)
    """
    if not root.exists():
        raise FileNotFoundError(f"Root folder not found: {root}")
    code_to_barcode: Dict[str, str] = {}
    malformed: List[str] = []
    counts: Counter = Counter()
    image_counts: Dict[str, int] = {}
    folder_paths: Dict[str, Path] = {}

    for entry in root.iterdir():
        if not entry.is_dir():
            continue
        code, bc = parse_folder(entry.name)
        if not code:
            malformed.append(entry.name)
            continue
        counts[code] += 1
        code_to_barcode[code] = bc
        folder_paths[code] = entry
        image_counts[code] = count_images_in_folder(entry)

    return code_to_barcode, malformed, counts, image_counts, folder_paths

def extract_csv_maps(rows, code_header: str, barcode_header: str):
    csv_codes: Set[str] = set()
    code_to_barcode: Dict[str, str] = {}
    counts: Counter = Counter()
    empty = 0
    for r in rows:
        code = (r.get(code_header) or "").strip().upper()
        if not code:
            empty += 1
            continue
        counts[code] += 1
        csv_codes.add(code)
        if barcode_header:
            code_to_barcode[code] = (r.get(barcode_header) or "").strip()
    if empty:
        print(f"[WARN] {empty} row(s) had empty '{code_header}' and were skipped.")
    return csv_codes, code_to_barcode, counts

def write_csv(path: Path, rows: List[Dict[str, str]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def main():
    base = script_dir()
    csv_path = base / CSV_BASENAME
    outdir = base / OUTDIR_NAME
    root = Path(ROOT_FOLDER)

    print("=== Startup ===")
    print(f"[INFO] Script dir: {base}")
    print(f"[INFO] CSV:        {csv_path}")
    print(f"[INFO] Root:       {root}")
    print(f"[INFO] Outdir:     {outdir}")

    rows, code_header, barcode_header = read_csv(csv_path)
    csv_codes, csv_code_to_barcode, csv_dup_counts = extract_csv_maps(rows, code_header, barcode_header)

    (folder_code_to_barcode,
     malformed,
     folder_dup_counts,
     image_counts,
     folder_paths) = scan_root(root)

    folder_codes = set(folder_code_to_barcode.keys())

    missing_codes = sorted(csv_codes - folder_codes)     # in CSV, not in folders
    extra_codes   = sorted(folder_codes - csv_codes)     # in folders, not in CSV
    common_codes  = sorted(csv_codes & folder_codes)

    # Barcode mismatches if barcode column exists
    mismatches = []
    if barcode_header:
        for code in common_codes:
            csv_bc = (csv_code_to_barcode.get(code) or "").strip()
            fld_bc = (folder_code_to_barcode.get(code) or "").strip()
            if csv_bc and fld_bc and csv_bc != fld_bc:
                mismatches.append((code, csv_bc, fld_bc))

    csv_dupes    = sorted([c for c, n in csv_dup_counts.items() if n > 1])
    folder_dupes = sorted([c for c, n in folder_dup_counts.items() if n > 1])

    # Image audits
    empty_folders = sorted([code for code in folder_codes if image_counts.get(code, 0) == 0])
    low_image_folders = sorted([code for code in folder_codes if 0 < image_counts.get(code, 0) < MIN_IMAGES_PER_FOLDER])

    # --- Write reports ---
    outdir.mkdir(parents=True, exist_ok=True)

    # 1) Missing: write full CSV rows
    if missing_codes:
        miss_rows = [r for r in rows if (r.get(code_header) or "").strip().upper() in set(missing_codes)]
        write_csv(outdir / "missing_in_folders.csv", miss_rows, list(rows[0].keys()))

    # 2) Extra folders
    if extra_codes:
        write_csv(outdir / "extra_folders.csv",
                  [{"Code": c, "Folder Barcode": folder_code_to_barcode.get(c, ""), "Folder Path": str(folder_paths.get(c, ""))}
                   for c in extra_codes],
                  ["Code", "Folder Barcode", "Folder Path"])

    # 3) Malformed folder names
    if malformed:
        (outdir / "malformed_folders.txt").write_text("\n".join(malformed), encoding="utf-8")

    # 4) Duplicates
    if csv_dupes:
        write_csv(outdir / "duplicates_in_csv.csv",
                  [{"Code": c, "Count": csv_dup_counts[c]} for c in csv_dupes],
                  ["Code", "Count"])
    if folder_dupes:
        write_csv(outdir / "duplicates_in_folders.csv",
                  [{"Code": c, "Count": folder_dup_counts[c]} for c in folder_dupes],
                  ["Code", "Count"])

    # 5) Barcode mismatches
    if mismatches:
        write_csv(outdir / "barcode_mismatches.csv",
                  [{"Code": code, "CSV Barcode": csv_bc, "Folder Barcode": fld_bc}
                   for code, csv_bc, fld_bc in mismatches],
                  ["Code", "CSV Barcode", "Folder Barcode"])

    # 6) Empty & low-image folders
    if empty_folders:
        write_csv(outdir / "empty_folders.csv",
                  [{"Code": c, "Folder Path": str(folder_paths[c])} for c in empty_folders],
                  ["Code", "Folder Path"])
    if low_image_folders:
        write_csv(outdir / "folders_with_few_images.csv",
                  [{"Code": c, "Image Count": image_counts[c], "Folder Path": str(folder_paths[c])}
                   for c in low_image_folders],
                  ["Code", "Image Count", "Folder Path"])

    # --- Summary ---
    print("\n=== Summary ===")
    print(f"CSV codes:                 {len(csv_codes)}")
    print(f"Folder codes:              {len(folder_codes)}")
    print(f"Found in both:             {len(common_codes)}")
    print(f"Missing (CSV → no folder): {len(missing_codes)}")
    print(f"Extra (folder → no CSV):   {len(extra_codes)}")
    print(f"Malformed folders:         {len(malformed)}")
    print(f"Duplicates in CSV:         {len(csv_dupes)}")
    print(f"Duplicates in folders:     {len(folder_dupes)}")
    if barcode_header:
        print(f"Barcode mismatches:        {len(mismatches)}")
    else:
        print("No barcode column detected in CSV; skipping barcode cross-checks.")
    print(f"Empty folders:             {len(empty_folders)}")
    print(f"Folders with < {MIN_IMAGES_PER_FOLDER} images: {len(low_image_folders)}")

    # Peeks
    if missing_codes:
        print("\nFirst few missing codes:")
        for c in missing_codes[:10]:
            print("  -", c)
    if extra_codes:
        print("\nFirst few extra folder codes:")
        for c in extra_codes[:10]:
            print("  -", c)
    if malformed:
        print("\nFirst few malformed folder names:")
        for n in malformed[:10]:
            print("  -", n)
    if low_image_folders:
        print("\nFirst few folders with few images:")
        for c in low_image_folders[:10]:
            print(f"  - {c}: {image_counts[c]} files")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[FATAL] {type(e).__name__}: {e}")
        sys.exit(1)
