#!/usr/bin/env python3
r"""
Read folders from a CSV (first column 'folder'), look for matching subfolders
inside SRC_ROOT, and copy any matches to DEST_ROOT.

- CSV location: next to this script, file name: empty_folders.csv
- Source root:  C:\Roshaan\iCloudRenamedConvertedBackup
- Dest root:    C:\Roshaan\remaining

Behaviour:
- Only immediate subfolders of SRC_ROOT are considered for matching.
- Folder names must match exactly (case-insensitive on Windows).
- If a destination folder with the same name already exists, a numeric suffix
  (_1, _2, ...) is appended to avoid collisions.
- Prints a summary of copied / missing / skipped items.

You can safely re-run this; already-copied names will get a suffix.
"""

import csv
import os
import shutil
import sys
from typing import List, Set


# =========================
# 🔧 SETTINGS
# =========================
SRC_ROOT  = r"C:\Roshaan\iCloudRenamedConverted"
DEST_ROOT = r"C:\Roshaan\remaining"
CSV_BASENAME = "empty_folders.csv"  # CSV sits next to this script
# =========================


def script_dir() -> str:
    try:
        return os.path.dirname(os.path.abspath(__file__))
    except NameError:
        return os.getcwd()


def read_folder_names(csv_path: str) -> List[str]:
    """
    Read the CSV and return the list of folder names from the first column named 'folder'
    (case-insensitive, ignores blank values).
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found at: {csv_path}")

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("CSV has no headers. Expected a 'folder' column.")
        # Find the 'folder' header case-insensitively
        header_map = { (h or "").strip().lower(): h for h in reader.fieldnames }
        if "folder" not in header_map:
            raise KeyError(f"CSV is missing a 'folder' column. Headers found: {reader.fieldnames}")
        folder_col = header_map["folder"]

        names: List[str] = []
        for row in reader:
            val = (row.get(folder_col) or "").strip()
            if val:
                names.append(val)
    return names


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def unique_dest_path(dest_root: str, folder_name: str) -> str:
    """
    Compute a unique destination path inside 'dest_root' for a folder called 'folder_name'.
    If a clash exists, appends _1, _2, ... to the name.
    """
    candidate = os.path.join(dest_root, folder_name)
    if not os.path.exists(candidate):
        return candidate

    base = folder_name
    i = 1
    while True:
        candidate = os.path.join(dest_root, f"{base}_{i}")
        if not os.path.exists(candidate):
            return candidate
        i += 1


def main() -> None:
    base = script_dir()
    csv_path = os.path.join(base, CSV_BASENAME)

    print("=== Copy Folders Listed in CSV ===")
    print(f"[INFO] CSV:       {csv_path}")
    print(f"[INFO] SRC_ROOT:  {SRC_ROOT}")
    print(f"[INFO] DEST_ROOT: {DEST_ROOT}")

    # Basic checks
    if not os.path.exists(SRC_ROOT):
        print(f"[FATAL] Source root does not exist: {SRC_ROOT}")
        sys.exit(1)

    ensure_dir(DEST_ROOT)

    try:
        wanted_names = read_folder_names(csv_path)
    except Exception as e:
        print(f"[FATAL] {type(e).__name__}: {e}")
        sys.exit(1)

    # Index immediate subfolders of SRC_ROOT by name (case-insensitive map to real path)
    try:
        entries = [e for e in os.scandir(SRC_ROOT) if e.is_dir()]
    except Exception as e:
        print(f"[FATAL] Cannot scan source root: {e}")
        sys.exit(1)

    name_to_path = { e.name: e.path for e in entries }
    # On Windows, filesystem is case-insensitive; still normalise to be robust
    lower_index = { name.lower(): name for name in name_to_path.keys() }

    copied: List[str] = []
    missing: List[str] = []
    skipped_existing: List[str] = []  # if the exact dest already existed without room for suffix (unlikely)

    for want in wanted_names:
        want_stripped = want.strip().rstrip("\\/")  # tolerate accidental slash
        key_lower = want_stripped.lower()

        if key_lower not in lower_index:
            missing.append(want_stripped)
            print(f"[MISS ] {want_stripped} (not found in source)")
            continue

        real_name = lower_index[key_lower]
        src_path = name_to_path[real_name]

        # Compute unique destination path (with suffix if needed)
        dest_path = unique_dest_path(DEST_ROOT, real_name)

        try:
            shutil.copytree(src_path, dest_path)
            copied.append(real_name)
            print(f"[COPIED] {real_name}  ->  {dest_path}")
        except FileExistsError:
            # Extremely rare due to unique_dest_path; still handle defensively
            skipped_existing.append(real_name)
            print(f"[SKIP ] Destination already exists and no unique name found: {dest_path}")
        except Exception as e:
            print(f"[ERROR] Failed to copy '{real_name}': {e}")

    # Summary
    print("\n=== Summary ===")
    print(f"Requested from CSV: {len(wanted_names)}")
    print(f"Copied:             {len(copied)}")
    print(f"Missing:            {len(missing)}")
    if missing:
        for m in missing:
            print(f"  - {m}")
    if skipped_existing:
        print(f"Skipped (existing dest): {len(skipped_existing)}")
        for s in skipped_existing:
            print(f"  - {s}")


if __name__ == "__main__":
    main()
