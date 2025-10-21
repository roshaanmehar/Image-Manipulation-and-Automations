#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, csv, os, re, shutil, sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional

# ---- SOURCE PATHS YOU GAVE ----
BARCODES_SRC = r"C:\Roshaan\Code\Barcode Generation\barcodes_master"
IMAGES_SRC_ROOT = r"C:\Roshaan\iCloudSinglesConverted"

# ---- OUTPUT NAMES (relative to PWD) ----
MASTER_BARCODES_DIRNAME = "barcodes_master"
MASTER_IMAGES_DIRNAME = "images"
BY_CATEGORY_DIRNAME = "csv_by_category"
MASTER_CSV_NAME = "master.csv"
LOG_CSV_NAME = "processing_log.csv"
FIELDS_TXT = "fields_used.txt"

# ---- COLUMNS: removals and required keeps ----
# We’ll remove these (case/spacing tolerant)
REMOVE_THESE = {
    "supp code", "#", "num", "taken", "form  coded", "image", "outer box barcode",
    "cm cost (ex vat) company cost price", "cm margin",
    "d cost (exvat)", "d cost (incvat)", "trade margin",
    "re-sell margin", "rrp (ex vat)", "rrp (inc vat)", "margin",
    "rrp ex vat", "rrp inc vat",  # the earlier pair you *don’t* want
    "new rrp evat", "new rrp incvat",
}
# Columns you explicitly want kept (in addition to anything not removed)
EXPLICIT_KEEPS = {
    "cost (ex vat  trade price)",  # handles the double-space case
    "cost (inc vat)",
    "rrp ex vat inc",
    "rrp inc vat retail price",
    "code (cm)", "unit barcode", "name / description", "category"
}

def ncol(s: str) -> str:
    s = s.replace("£", "")
    s = re.sub(r"[\"']", "", s)
    return s.strip()

def normalise_header(h: str) -> str:
    x = h.lower()
    x = re.sub(r"\s+", " ", x)
    x = x.replace(".", "").replace(",", "").replace("+", "").replace("(", "").replace(")", "").replace("/", " / ")
    x = re.sub(r"\s+", " ", x)
    x = x.replace("ex. vat", "ex vat").replace("inc. vat", "inc vat")
    x = x.strip()
    return x

def should_remove(h: str) -> bool:
    nh = normalise_header(h)
    if nh in EXPLICIT_KEEPS:
        return False
    return nh in REMOVE_THESE or nh in {"", "#"}

def is_cost_keep(h: str) -> bool:
    nh = normalise_header(h)
    return nh.startswith("cost (ex vat") or nh.startswith("cost (inc vat")

def clean_value_for_indesign(v: str) -> str:
    if v is None:
        return ""
    v = v.replace("£", "")
    v = v.replace('"', "").replace("'", "")
    return v.strip()

def sanitise_category(cat: str) -> str:
    if not cat:
        return "Uncategorised"
    cleaned = re.sub(r"[^A-Za-z0-9]", "", cat)
    return cleaned or "Uncategorised"

def first_file_in_folder(folder: Path) -> Optional[Path]:
    if not folder.is_dir():
        return None
    files = sorted([p for p in folder.iterdir() if p.is_file()])
    return files[0] if files else None

def find_images_folder_for_itemcode(itemcode: str) -> Optional[Path]:
    root = Path(IMAGES_SRC_ROOT)
    if not root.is_dir():
        return None
    want = (itemcode or "").upper() + " -"
    for child in root.iterdir():
        if child.is_dir() and child.name.upper().startswith(want):
            return child
    return None

def copy_if_exists(src: Path, dest: Path) -> bool:
    try:
        if src.is_file():
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dest)
            return True
    except Exception:
        pass
    return False

def log_issue(writer, rownum, code_cm, unit_barcode, issue, details=""):
    writer.writerow({
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "row_number": rownum,
        "code_cm": code_cm,
        "unit_barcode": unit_barcode,
        "issue": issue,
        "details": details
    })

def process(input_csv: Path, out_root: Path):
    by_cat = out_root / BY_CATEGORY_DIRNAME
    mbar = out_root / MASTER_BARCODES_DIRNAME
    mimg = out_root / MASTER_IMAGES_DIRNAME
    by_cat.mkdir(parents=True, exist_ok=True)
    mbar.mkdir(parents=True, exist_ok=True)
    mimg.mkdir(parents=True, exist_ok=True)

    log_path = out_root / LOG_CSV_NAME
    with open(log_path, "w", newline="", encoding="utf-8") as logfp:
        logw = csv.DictWriter(logfp, fieldnames=["timestamp","row_number","code_cm","unit_barcode","issue","details"])
        logw.writeheader()

        # read input
        try:
            src = open(input_csv, "r", newline="", encoding="utf-8-sig")
        except UnicodeDecodeError:
            src = open(input_csv, "r", newline="", encoding="cp1252")
        with src:
            reader = csv.DictReader(src)
            raw_headers = reader.fieldnames or []

            # build final header list (preserving original order)
            final_headers: List[str] = []
            for h in raw_headers:
                if should_remove(h):
                    continue
                final_headers.append(h)
            # ensure explicit keeps present if they existed in input
            for h in raw_headers:
                if normalise_header(h) in EXPLICIT_KEEPS and h not in final_headers:
                    final_headers.append(h)
            # be extra sure we keep the cost columns even if names slightly vary
            for h in raw_headers:
                if is_cost_keep(h) and h not in final_headers:
                    final_headers.append(h)

            # ensure key columns are first if present
            must_first = [c for c in ["Code (CM)", "Name / Description", "Unit Barcode", "Category"] if c in final_headers]
            final_headers = must_first + [h for h in final_headers if h not in must_first]

            # add our two fields (exact names)
            final_headers_with_added = final_headers + ["@barcodeImages", "@images"]

            # write the header list to a helper txt for quick InDesign check
            with open(out_root / FIELDS_TXT, "w", encoding="utf-8") as ftxt:
                ftxt.write("\n".join(final_headers_with_added))

            master_csv = out_root / MASTER_CSV_NAME
            # QUOTE_NONE avoids literal quotes in the file, which you prefer for Data Merge.
            # WARNING: if any *cell* contains a comma, that will create extra columns.
            masterfp = open(master_csv, "w", newline="", encoding="utf-8")
            masterw = csv.DictWriter(masterfp, fieldnames=final_headers_with_added,
                                     quoting=csv.QUOTE_NONE, escapechar="\\")
            masterw.writeheader()

            # cache for per-category writers
            cat_writers: Dict[str, Tuple[csv.DictWriter, any, Path, Path]] = {}

            for idx, row in enumerate(reader, start=2):
                code_cm = (row.get("Code (CM)") or "").strip()
                unit_barcode = (row.get("Unit Barcode") or "").strip()
                category = (row.get("Category") or "").strip()

                out_row = {}
                for h in final_headers:
                    out_row[h] = clean_value_for_indesign(str(row.get(h, "")))

                # barcode
                barcode_rel = ""
                if unit_barcode:
                    src_b = Path(BARCODES_SRC) / f"{unit_barcode}.png"
                    dst_b = mbar / f"{unit_barcode}.png"
                    if copy_if_exists(src_b, dst_b):
                        barcode_rel = f"{MASTER_BARCODES_DIRNAME}/{unit_barcode}.png"
                    else:
                        log_issue(logw, idx, code_cm, unit_barcode, "BARCODE_MISSING", str(src_b))
                else:
                    log_issue(logw, idx, code_cm, unit_barcode, "BARCODE_EMPTY")

                # product image
                image_rel = ""
                if code_cm:
                    folder = find_images_folder_for_itemcode(code_cm)
                    if folder:
                        f = first_file_in_folder(folder)
                        if f:
                            safe_name = clean_value_for_indesign(f.name)
                            dst_i = mimg / safe_name
                            if copy_if_exists(f, dst_i):
                                image_rel = f"{MASTER_IMAGES_DIRNAME}/{safe_name}"
                            else:
                                log_issue(logw, idx, code_cm, unit_barcode, "IMAGE_COPY_FAILED", f"{f} -> {dst_i}")
                        else:
                            log_issue(logw, idx, code_cm, unit_barcode, "IMAGE_FILE_NOT_FOUND", str(folder))
                    else:
                        log_issue(logw, idx, code_cm, unit_barcode, "IMAGE_FOLDER_NOT_FOUND")
                else:
                    log_issue(logw, idx, code_cm, unit_barcode, "CODE_CM_EMPTY")

                out_row["@barcodeImages"] = barcode_rel
                out_row["@images"] = image_rel
                masterw.writerow(out_row)

                # per-category
                catname = sanitise_category(category)
                catroot = (out_root / BY_CATEGORY_DIRNAME / catname)
                catroot.mkdir(parents=True, exist_ok=True)
                catcsv = catroot / f"{catname}.csv"
                catbar = catroot / MASTER_BARCODES_DIRNAME
                catimg = catroot / MASTER_IMAGES_DIRNAME
                catbar.mkdir(parents=True, exist_ok=True)
                catimg.mkdir(parents=True, exist_ok=True)

                if catname not in cat_writers:
                    catfp = open(catcsv, "w", newline="", encoding="utf-8")
                    catw = csv.DictWriter(catfp, fieldnames=final_headers_with_added,
                                          quoting=csv.QUOTE_NONE, escapechar="\\")
                    catw.writeheader()
                    cat_writers[catname] = (catw, catfp, catbar, catimg)

                catw, _, catbar, catimg = cat_writers[catname]
                cat_row = dict(out_row)

                if unit_barcode:
                    src_in_master = mbar / f"{unit_barcode}.png"
                    dst_in_cat = catbar / f"{unit_barcode}.png"
                    if src_in_master.is_file():
                        copy_if_exists(src_in_master, dst_in_cat)
                        cat_row["@barcodeImages"] = f"{MASTER_BARCODES_DIRNAME}/{unit_barcode}.png"
                if image_rel:
                    img_name = Path(image_rel).name
                    src_in_master_img = mimg / img_name
                    dst_in_cat_img = catimg / img_name
                    if src_in_master_img.is_file():
                        copy_if_exists(src_in_master_img, dst_in_cat_img)
                        cat_row["@images"] = f"{MASTER_IMAGES_DIRNAME}/{img_name}"

                catw.writerow(cat_row)

            for _, (_, fp, _, _) in cat_writers.items():
                fp.close()
            masterfp.close()

    print("Done.\n- master.csv\n- csv_by_category/<Category>/<Category>.csv\n- barcodes_master/, images/\n- processing_log.csv\n- fields_used.txt")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("input_csv")
    args = p.parse_args()
    inpath = Path(args.input_csv).expanduser().resolve()
    if not inpath.is_file():
        print(f"Input CSV not found: {inpath}", file=sys.stderr); sys.exit(1)
    process(inpath, Path.cwd())

if __name__ == "__main__":
    main()
