import csv
import shutil
from pathlib import Path

# === CONSTANTS ===
ROOT_DIR = Path(__file__).resolve().parent      # root = where this script lives
FRONT45_DIR = Path(r"C:\Roshaan\front45")       # source of *_front_45_raw.png files
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

    image_index = build_index(FRONT45_DIR)

    copied = 0
    missing = 0
    missing_images = []

    with csv_path.open("r", encoding="utf-8-sig", newline="") as infile:
        # restkey catches overflow columns instead of using None
        reader = csv.DictReader(infile, restkey="_EXTRA", restval="")
        fieldnames = [fn for fn in (reader.fieldnames or []) if fn is not None]

        required = {"Code (CM)", "@images", "@barcodeImages"}
        if not required.issubset(set(fieldnames)):
            print(f"[ERROR] Missing required columns. Found: {fieldnames}")
            return

        with cleaned_csv_path.open("w", encoding="utf-8", newline="") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                # ignore overflow fields entirely
                row.pop("_EXTRA", None)
                # keep only known header fields
                safe_row = {k: row.get(k, "") for k in fieldnames}

                # clean columns
                safe_row["@images"] = clean_prefix(safe_row.get("@images", ""), "images/")
                safe_row["@barcodeImages"] = clean_prefix(safe_row.get("@barcodeImages", ""), "barcodes_master/")

                # copy image if exists
                code = (safe_row.get("Code (CM)") or "").strip()
                if code:
                    expected_name = f"{code}{EXPECTED_SUFFIX}".lower()
                    src = image_index.get(expected_name)
                    if src:
                        dest = folder_path / src.name
                        try:
                            shutil.copy2(src, dest)   # overwrite if present
                            copied += 1
                        except Exception as e:
                            print(f"[WARN] Copy failed for {src.name}: {e}")
                    else:
                        missing += 1
                        missing_images.append(expected_name)
                writer.writerow(safe_row)

    # Write missing image names to a log
    if missing_images:
        with missing_log_path.open("w", encoding="utf-8") as log_file:
            log_file.write("Missing images (not found in front45):\n")
            for img in missing_images:
                log_file.write(f"{img}\n")

    print(f"[DONE] {csv_path.name} processed.")
    print(f"→ Copied images : {copied}")
    print(f"→ Missing images: {missing} (logged in {missing_log_path})")

if __name__ == "__main__":
    folder = input("Folder in root (e.g. ENAMELWARE): ").strip()
    csv_file = input("CSV filename (leave blank to auto-pick): ").strip()
    process_csv(folder, csv_file)
