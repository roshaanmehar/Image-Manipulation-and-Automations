import csv
import shutil
from pathlib import Path

# === CONSTANTS ===
ROOT_DIR = Path(__file__).resolve().parent      # ← root = where this script lives
FRONT45_DIR = Path(r"C:\Roshaan\front45")       # Source of *_front_45_raw.png files
EXPECTED_SUFFIX = "_front_45_raw.png"

def clean_prefix(value: str, prefix: str) -> str:
    if not value:
        return value
    value = value.strip()
    return value[len(prefix):] if value.startswith(prefix) else value

def build_index(folder: Path):
    if not folder.exists():
        print(f"[ERROR] Image source not found: {folder}")
        return {}
    return {p.name.lower(): p for p in folder.iterdir() if p.is_file()}

def resolve_csv_path(folder_name: str, csv_filename: str) -> Path:
    """Resolve the CSV inside ROOT_DIR/<folder_name>. If csv_filename is empty, try to guess."""
    folder_path = ROOT_DIR / folder_name
    if not folder_path.exists():
        raise FileNotFoundError(f"Folder not found: {folder_path}")

    if csv_filename:
        path = folder_path / csv_filename
        if path.exists():
            return path

    # Fallback: find the first .csv in the folder (case-insensitive)
    candidates = list(folder_path.glob("*.csv")) + list(folder_path.glob("*.CSV"))
    if candidates:
        return candidates[0]

    raise FileNotFoundError(f"No CSV found in {folder_path}")

def process_csv(folder_name: str, csv_filename: str):
    csv_path = resolve_csv_path(folder_name, csv_filename.strip())
    folder_path = csv_path.parent
    cleaned_csv_path = csv_path.with_name(csv_path.stem + "_cleaned.csv")

    print(f"[INFO] Root dir      : {ROOT_DIR}")
    print(f"[INFO] Using CSV     : {csv_path}")
    print(f"[INFO] Output (clean): {cleaned_csv_path}")
    print(f"[INFO] Images source : {FRONT45_DIR}")

    image_index = build_index(FRONT45_DIR)

    copied = 0
    missing = 0

    with csv_path.open("r", encoding="utf-8-sig", newline="") as infile, \
         cleaned_csv_path.open("w", encoding="utf-8", newline="") as outfile:

        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames or []
        required = {"Code (CM)", "@images", "@barcodeImages"}
        if not required.issubset(set(fieldnames)):
            print(f"[ERROR] Missing required columns. Found: {fieldnames}")
            return

        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            row["@images"] = clean_prefix(row.get("@images", ""), "images/")
            row["@barcodeImages"] = clean_prefix(row.get("@barcodeImages", ""), "barcodes_master/")

            code = (row.get("Code (CM)") or "").strip()
            if code:
                expected_name = f"{code}{EXPECTED_SUFFIX}".lower()
                src = image_index.get(expected_name)
                if src:
                    dest = folder_path / src.name          # paste into the same folder as the CSV
                    try:
                        shutil.copy2(src, dest)            # overwrite if present
                        copied += 1
                    except Exception as e:
                        print(f"[WARN] Copy failed for {src.name}: {e}")
                else:
                    missing += 1

            writer.writerow(row)

    print(f"[DONE] {csv_path.name} processed.")
    print(f"→ Copied images : {copied}")
    print(f"→ Missing images: {missing} (no matching file in {FRONT45_DIR})")

if __name__ == "__main__":
    folder = input("Folder in root (e.g. ENAMELWARE): ").strip()
    csv_file = input("CSV filename (leave blank to auto-pick): ").strip()
    process_csv(folder, csv_file)
