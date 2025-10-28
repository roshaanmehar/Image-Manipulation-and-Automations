#!/usr/bin/env python3
r"""
Scan subfolders inside a fixed ROOT_FOLDER. For each immediate child folder:
- If the folder is truly empty, move it to 'emptyfolders' (inside ROOT_FOLDER)
  and append its original name to 'empty_folders.csv' (in ROOT_FOLDER).
- If the folder has fewer than 4 image files, move it to 'lessthan4' (inside ROOT_FOLDER).
- Otherwise, leave it alone.

Notes:
- Only immediate subfolders of ROOT_FOLDER are considered.
- Image counting is NOT recursive by default (set COUNT_RECURSIVELY = True to include nested images).
- Destination folders are created if missing.
- If a destination folder with the same name exists, a numeric suffix (_1, _2, …) is added to avoid clashes.
"""

import csv
import os
import shutil
import sys
from datetime import datetime
from typing import Iterable, Set

# =========================
# 🔧 SETTINGS
# =========================
ROOT_FOLDER = r"C:\Roshaan\Code\nanoBananGen\output_images"

# Treat these as images (case-insensitive)
IMAGE_EXTS: Set[str] = {
    ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tif", ".tiff",
    ".webp", ".heic", ".heif", ".jfif"
}

# Folder names created/used within ROOT_FOLDER
LESS_THAN_4_DIR = "lessthan4"
EMPTY_FOLDERS_DIR = "emptyfolders"

# CSV log filename for empty folders (written in ROOT_FOLDER)
EMPTY_LOG_CSV = "empty_folders.csv"

# Whether to count images recursively within each subfolder
COUNT_RECURSIVELY = False
# =========================


def is_dir_empty(path: str) -> bool:
    """True if the directory contains no files or subdirectories at all."""
    with os.scandir(path) as it:
        for _ in it:
            return False
    return True


def iter_files(path: str, recursive: bool) -> Iterable[str]:
    """Yield full paths of files within 'path' (optionally recursive)."""
    if recursive:
        for root, _, files in os.walk(path):
            for f in files:
                yield os.path.join(root, f)
    else:
        with os.scandir(path) as it:
            for entry in it:
                if entry.is_file():
                    yield entry.path


def count_images_in_folder(path: str, recursive: bool) -> int:
    """Count how many image files are inside 'path' (optionally recursively)."""
    count = 0
    for fp in iter_files(path, recursive):
        _, ext = os.path.splitext(fp)
        if ext.lower() in IMAGE_EXTS:
            count += 1
    return count


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
    suffix = 1
    while True:
        candidate = os.path.join(dest_root, f"{base}_{suffix}")
        if not os.path.exists(candidate):
            return candidate
        suffix += 1


def move_folder(src_path: str, dest_root: str) -> str:
    """Move 'src_path' into 'dest_root', avoiding name collisions. Returns final dest path."""
    ensure_dir(dest_root)
    folder_name = os.path.basename(src_path.rstrip(os.sep))
    final_dest = unique_dest_path(dest_root, folder_name)
    shutil.move(src_path, final_dest)
    return final_dest


def log_empty_folder(csv_path: str, folder_rel: str) -> None:
    """Append the empty folder name and timestamp to the CSV log (creates with header if missing)."""
    exists = os.path.exists(csv_path)
    with open(csv_path, "a", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        if not exists:
            writer.writerow(["folder", "logged_at_iso"])
        writer.writerow([folder_rel, datetime.now().isoformat(timespec="seconds")])


def main() -> None:
    root = ROOT_FOLDER

    print("=== Folder Image Auditor (Fixed Root) ===")
    print(f"[INFO] ROOT_FOLDER: {root}")
    print(f"[INFO] Recursive image counting: {COUNT_RECURSIVELY}")
    print(f"[INFO] Image extensions: {sorted(IMAGE_EXTS)}")

    if not os.path.exists(root):
        print(f"[FATAL] ROOT_FOLDER path does not exist: {root}")
        sys.exit(1)

    # Prepare special directories & CSV paths (all inside ROOT_FOLDER)
    less_than_4_path = os.path.join(root, LESS_THAN_4_DIR)
    empty_folders_path = os.path.join(root, EMPTY_FOLDERS_DIR)
    empty_log_csv = os.path.join(root, EMPTY_LOG_CSV)

    ensure_dir(less_than_4_path)
    ensure_dir(empty_folders_path)

    # Skip these during scan
    skip_names: Set[str] = {
        LESS_THAN_4_DIR,
        EMPTY_FOLDERS_DIR,
        ".git",
        "__pycache__",
    }

    moved_lt4 = 0
    moved_empty = 0
    scanned = 0

    try:
        entries = list(os.scandir(root))
    except Exception as e:
        print(f"[FATAL] Cannot scan ROOT_FOLDER: {e}")
        sys.exit(1)

    for entry in entries:
        if not entry.is_dir():
            continue
        name = entry.name

        if name in skip_names or name.startswith("."):
            continue

        folder_path = entry.path
        scanned += 1

        # Handle truly empty folders first
        if is_dir_empty(folder_path):
            rel_name = os.path.relpath(folder_path, root)
            dest = move_folder(folder_path, empty_folders_path)
            log_empty_folder(empty_log_csv, rel_name)
            moved_empty += 1
            print(f"[EMPTY] {rel_name}  ->  {os.path.relpath(dest, root)}")
            continue

        # Count images (optionally recursive)
        img_count = count_images_in_folder(folder_path, COUNT_RECURSIVELY)

        if img_count < 4:
            rel_name = os.path.relpath(folder_path, root)
            dest = move_folder(folder_path, less_than_4_path)
            moved_lt4 += 1
            print(f"[LT4  ] {rel_name} (images={img_count})  ->  {os.path.relpath(dest, root)}")
        else:
            print(f"[OK   ] {name} (images={img_count})")

    print("\n=== Summary ===")
    print(f"Scanned folders:          {scanned}")
    print(f"Moved to 'emptyfolders':  {moved_empty}")
    print(f"Moved to 'lessthan4':     {moved_lt4}")
    if os.path.exists(empty_log_csv):
        print(f"[LOG ] Empty folders CSV: {empty_log_csv}")


if __name__ == "__main__":
    main()
