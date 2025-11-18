import csv
import shutil
from pathlib import Path

# === CONSTANTS ===
ROOT_DIR = Path(__file__).resolve().parent
FRONT45_DIR = Path(r"C:\Roshaan\front45")       # Source of *_front_45_raw.png files
BARCODES_DIR = Path(r"C:\Roshaan\Code\CSV InDesign\barcodes_master")  # Source of barcode PNGs
EXPECTED_SUFFIX = "_front_45_raw.png"           # Image filename pattern

def clean_prefix(value: str, prefix: str) -> str:
    if not value:
        return value
    value = value.strip()
    return value[len(prefix):] if value.startswith(prefix) else value

def build_index(folder: Path):
    """Build a case-insensitive index of filenames for quick lookups."""
    if not folder.exists():
        print(f"[ERROR] Image source not found: {folder}")
        return {}
    return {p.name.lower(): p for p in folder.iterdir() if p.is_file()}

def resolve_csv_path(folder_name: str, csv_filename: str) -> Path:
    folder_path = ROOT_DIR / folder_name
    if not folder_path.exists():
        raise FileNotFoundError(f"Folder not found: {folder_path}")

    if csv_filename:
        path = folder_path / csv_filename
        if path.exists():
            return path

    # auto-pick first CSV if not given
    candidates = list(folder_path.glob("*.csv")) + list(folder_path.glob("*.CSV"))
    if candidates:
        return candidates[0]

    raise FileNotFoundError(f"No CSV found in {folder_path}")

def process_csv(folder_name: str, csv_filename: str):
    csv_path = resolve_csv_path(folder_name, csv_filename.strip())
    folder_path = csv_path.parent
    cleaned_csv_path = csv_path.with_name(csv_path.stem + "_cleaned.csv")
    missing_log_path = folder_path / "missing_images.txt"

    print(f"[INFO] Root dir      : {ROOT_DIR}")
    print(f"[INFO] Using CSV     : {csv_path}")
    print(f"[INFO] Output (clean): {cleaned_csv_path}")
    print(f"[INFO] Images source : {FRONT45_DIR}")
    print(f"[INFO] Barcodes dir  : {BARCODES_DIR}")

    image_index = build_index(FRONT45_DIR)
    barcode_index = build_index(BARCODES_DIR)

    copied_images = 0
    copied_barcodes = 0
    missing_images = []

    with csv_path.open("r", encoding="utf-8-sig", newline="") as infile:
        reader = csv.DictReader(infile, restkey="_EXTRA", restval="")
        fieldnames = [fn for fn in (reader.fieldnames or []) if fn is not None]

        required = {"Code (CM)", "@images", "@barcodeImages", "Unit Barcode"}
        if not required.issubset(set(fieldnames)):
            print(f"[ERROR] Missing required columns. Found: {fieldnames}")
            return

        with cleaned_csv_path.open("w", encoding="utf-8", newline="") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                row.pop("_EXTRA", None)
                safe_row = {k: row.get(k, "") for k in fieldnames}

                # Clean prefixes in image columns
                safe_row["@images"] = clean_prefix(safe_row.get("@images", ""), "images/")
                safe_row["@barcodeImages"] = clean_prefix(safe_row.get("@barcodeImages", ""), "barcodes_master/")

                # --- Copy product image ---
                code = (safe_row.get("Code (CM)") or "").strip()
                if code:
                    expected_name = f"{code}{EXPECTED_SUFFIX}".lower()
                    src = image_index.get(expected_name)
                    if src:
                        dest = folder_path / src.name
                        try:
                            shutil.copy2(src, dest)
                            copied_images += 1
                            safe_row["@images"] = src.name
                        except Exception as e:
                            print(f"[WARN] Copy failed for {src.name}: {e}")
                    else:
                        missing_images.append(expected_name)
                        safe_row["@images"] = ""
                else:
                    safe_row["@images"] = ""

                # --- Copy barcode image ---
                barcode = (safe_row.get("Unit Barcode") or "").strip()
                if barcode:
                    barcode_filename = f"{barcode}.png".lower()
                    src_barcode = barcode_index.get(barcode_filename)
                    if src_barcode:
                        dest_barcode = folder_path / src_barcode.name
                        try:
                            shutil.copy2(src_barcode, dest_barcode)
                            copied_barcodes += 1
                            safe_row["@barcodeImages"] = src_barcode.name
                        except Exception as e:
                            print(f"[WARN] Copy failed for {src_barcode.name}: {e}")
                    else:
                        missing_images.append(barcode_filename)
                        safe_row["@barcodeImages"] = ""
                else:
                    safe_row["@barcodeImages"] = ""

                writer.writerow(safe_row)

    # Write missing images & barcodes to a log
    if missing_images:
        with missing_log_path.open("w", encoding="utf-8") as log_file:
            log_file.write("Missing images/barcodes (not found in sources):\n")
            for img in missing_images:
                log_file.write(f"{img}\n")

    print(f"[DONE] {csv_path.name} processed.")
    print(f"→ Copied product images : {copied_images}")
    print(f"→ Copied barcode images  : {copied_barcodes}")
    print(f"→ Missing items          : {len(missing_images)} (logged in {missing_log_path})")

if __name__ == "__main__":
    folder = input("Folder in root (e.g. ENAMELWARE): ").strip()
    csv_file = input("CSV filename (leave blank to auto-pick): ").strip()
    process_csv(folder, csv_file)
