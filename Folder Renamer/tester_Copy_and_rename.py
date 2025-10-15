#!/usr/bin/env python3
import argparse
import csv
import os
from pathlib import Path
import shutil
import sys

# replace your IMAGE_EXTS line with this:
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".heic", ".webp", ".tif", ".tiff", ".bmp", ".dng"}

def load_mapping(csv_path, barcode_col="barcode", itemcode_col="itemcode"):
    """
    Load a mapping of barcode -> itemcode from CSV.
    The CSV must have headers; column names are configurable.
    Both keys and values are kept as strings (leading zeros preserved).
    """
    mapping = {}
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        # normalize header names to lower/strip
        headers = {h.lower().strip(): h for h in reader.fieldnames or []}
        if barcode_col not in headers or itemcode_col not in headers:
            raise ValueError(
                f"CSV must contain columns '{barcode_col}' and '{itemcode_col}'. "
                f"Found: {list(headers.keys())}"
            )
        for row in reader:
            bc = (row[headers[barcode_col]] or "").strip()
            ic = (row[headers[itemcode_col]] or "").strip()
            if bc and ic:
                mapping[bc] = ic
    return mapping

def safe_copy(src: Path, dst: Path):
    """
    Copy file from src to dst. If dst exists, append _v2, _v3, ... before the extension.
    """
    target = dst
    if target.exists():
        stem, ext = target.stem, target.suffix
        i = 2
        while True:
            alt = target.with_name(f"{stem}_v{i}{ext}")
            if not alt.exists():
                target = alt
                break
            i += 1
    shutil.copy2(src, target)
    return target

def process_folder(barcode_folder: Path, itemcode: str, out_root: Path, dry_run=False, max_images=None):
    """
    Copy & rename images inside a single barcode-named folder to:
    <out_root>/<itemcode> - <barcode>/<itemcode>_<n>.<ext>
    """
    barcode = barcode_folder.name
    dest_dir = out_root / f"{itemcode} - {barcode}"
    if not dry_run:
        dest_dir.mkdir(parents=True, exist_ok=True)

    # Enumerate images in a stable order
    files = sorted(
        [p for p in barcode_folder.iterdir() if p.is_file() and p.suffix.lower() in IMAGE_EXTS],
        key=lambda p: p.name.lower()
    )
    if max_images is not None:
        files = files[:max_images]

    copied = []
    for idx, src in enumerate(files, start=1):
        dest_name = f"{itemcode}_{idx}{src.suffix.lower()}"
        dest_path = dest_dir / dest_name
        if dry_run:
            copied.append((src, dest_path))
        else:
            final_path = safe_copy(src, dest_path)
            copied.append((src, final_path))
    return dest_dir, copied

def main():
    parser = argparse.ArgumentParser(description="Tester: copy & rename product images using barcode→itemcode mapping.")
    parser.add_argument("--src", required=True, help="Path to the folder containing barcode-named subfolders.")
    parser.add_argument("--csv", required=True, help="Path to CSV with mapping (barcode,itemcode).")
    parser.add_argument("--out", required=True, help="Destination root folder (new copies go here).")
    parser.add_argument("--barcode-col", default="barcode", help="CSV column name for barcode. Default: barcode")
    parser.add_argument("--itemcode-col", default="itemcode", help="CSV column name for itemcode. Default: itemcode")
    parser.add_argument("--tester-limit", type=int, default=3, help="Process only this many folders (tester mode). Default: 3")
    parser.add_argument("--max-images", type=int, default=None, help="Limit images per folder (for quick testing).")
    parser.add_argument("--include-nonimages", action="store_true", help="List non-image files found (never copied).")
    parser.add_argument("--dry-run", action="store_true", help="Show what would happen without copying.")
    parser.add_argument("--resume", action="store_true", help="Skip folders whose destination already exists and is non-empty.")
    args = parser.parse_args()

    src_root = Path(args.src).expanduser().resolve()
    out_root = Path(args.out).expanduser().resolve()
    csv_path = Path(args.csv).expanduser().resolve()

    if not src_root.is_dir():
        print(f"ERROR: Source folder not found: {src_root}", file=sys.stderr)
        sys.exit(1)
    if not csv_path.is_file():
        print(f"ERROR: CSV not found: {csv_path}", file=sys.stderr)
        sys.exit(1)
    if not args.dry_run:
        out_root.mkdir(parents=True, exist_ok=True)

    mapping = load_mapping(csv_path, barcode_col=args.barcode_col.lower().strip(),
                           itemcode_col=args.itemcode_col.lower().strip())
    print(f"Loaded {len(mapping)} barcode→itemcode mappings from CSV.")

    # Collect barcode folders
    barcode_folders = sorted([p for p in src_root.iterdir() if p.is_dir()], key=lambda p: p.name)
    total = len(barcode_folders)
    if args.tester_limit and args.tester_limit > 0:
        barcode_folders = barcode_folders[:args.tester_limit]

    print(f"Found {total} folders. Tester will process {len(barcode_folders)} folder(s):")
    for p in barcode_folders:
        print(f"  - {p.name}")

    missing = []
    processed = 0
    copied_files = 0

    non_images_seen = 0

    for folder in barcode_folders:
        barcode = folder.name
        itemcode = mapping.get(barcode)
        if not itemcode:
            missing.append(barcode)
            print(f"[SKIP] No mapping for barcode: {barcode}")
            continue

        dest_dir = out_root / f"{itemcode} - {barcode}"
        if args.resume and dest_dir.exists():
            # consider it "done" if it has at least one file
            has_files = any(dest_dir.iterdir())
            if has_files:
                print(f"[RESUME] Destination exists with files, skipping: {dest_dir.name}")
                continue

        if args.include_nonimages:
            others = [p for p in folder.iterdir() if p.is_file() and p.suffix.lower() not in IMAGE_EXTS]
            if others:
                non_images_seen += len(others)
                print(f"[NOTE] Non-image files in {barcode}: {[x.name for x in others]}")

        dest, copies = process_folder(
            barcode_folder=folder,
            itemcode=itemcode,
            out_root=out_root,
            dry_run=args.dry_run,
            max_images=args.max_images
        )
        processed += 1
        copied_files += len(copies)
        print(f"[OK] {folder.name} -> {dest.name} | files: {len(copies)}")
        if args.dry_run and copies:
            # show first few planned copies
            preview = copies[:min(3, len(copies))]
            for src, dst in preview:
                print(f"      would copy: {src.name} -> {dst.name}")
            if len(copies) > len(preview):
                print(f"      ... and {len(copies) - len(preview)} more")

    # Write missing mappings report
    if missing:
        miss_path = out_root / "missing_mappings.csv"
        if args.dry_run:
            print(f"[REPORT] {len(missing)} folder(s) with no mapping. (dry-run: not writing) Example(s): {missing[:5]}")
        else:
            with open(miss_path, "w", encoding="utf-8", newline="") as f:
                w = csv.writer(f)
                w.writerow(["barcode_without_mapping"])
                for bc in missing:
                    w.writerow([bc])
            print(f"[REPORT] Wrote missing mappings to: {miss_path}")

    print("\nSummary")
    print("-------")
    print(f"Processed folders: {processed}")
    print(f"Copied files:      {copied_files}{' (dry-run)' if args.dry_run else ''}")
    if non_images_seen and args.include_nonimages:
        print(f"Non-image files encountered (not copied): {non_images_seen}")

if __name__ == "__main__":
    main()
