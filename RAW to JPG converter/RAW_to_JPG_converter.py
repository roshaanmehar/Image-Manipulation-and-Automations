#!/usr/bin/env python3
import os
import sys
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

# >>> EDIT THESE <<< -----------------------------------------------------------
SOURCE_ROOT = r"C:\Roshaan\raw_renamed_500"      # change me
DEST_ROOT   = r"C:\Roshaan\converted_renamed_500"     # change me
# -----------------------------------------------------------------------------

VALID_EXTS = {".heic", ".dng"}  # file types to convert
OVERWRITE = True  # whether to overwrite existing JPGs

# Push it: large number of workers
MAX_WORKERS = max(32, (os.cpu_count() or 16) * 2)

IM_ARGS = ["-auto-orient", "-colorspace", "sRGB", "-quality", "90"]

# Logging setup
ts = datetime.now().strftime("%Y%m%d-%H%M%S")
DEST_ROOT_PATH = Path(DEST_ROOT)
LOG_DIR = DEST_ROOT_PATH / "_conversion_logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"convert_log_{ts}.txt"

unexpected_exts = set()

def log(msg: str):
    msg = msg.rstrip()
    print(msg)
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(msg + "\n")

def is_valid_image(path: Path) -> bool:
    return path.suffix.lower() in VALID_EXTS

def dest_from_source(src: Path) -> Path:
    rel = src.relative_to(SOURCE_ROOT)
    dest_rel = rel.with_suffix(".jpg")
    return Path(DEST_ROOT) / dest_rel

def ensure_dest_dir(dest_path: Path):
    dest_path.parent.mkdir(parents=True, exist_ok=True)

def has_conflict(src: Path, dest: Path) -> Path:
    if dest.exists():
        if OVERWRITE:
            return dest
        base = dest.with_suffix("")
        ext = dest.suffix
        i = 1
        new_dest = Path(f"{base}_{i}{ext}")
        while new_dest.exists():
            i += 1
            new_dest = Path(f"{base}_{i}{ext}")
        return new_dest
    return dest

def run_imagemagick_convert(src: Path, dest: Path) -> int:
    cmd = ["magick", str(src), *IM_ARGS, str(dest)]
    try:
        result = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False
        )
        if result.returncode != 0:
            log(f"[ERROR] {src} -> {dest} | IM exit {result.returncode}: {result.stderr.decode(errors='ignore').strip()}")
        return result.returncode
    except FileNotFoundError:
        log("ImageMagick 'magick' executable not found. Ensure ImageMagick is installed and on PATH.")
        return 127
    except Exception as e:
        log(f"[EXCEPTION] {src} -> {dest} | {e}")
        return 1

def process_file(src: Path) -> tuple[Path, Path, bool, str]:
    dest = dest_from_source(src)
    ensure_dest_dir(dest)
    dest = has_conflict(src, dest)
    if dest.exists() and not OVERWRITE:
        return (src, dest, True, "skip_exists")
    code = run_imagemagick_convert(src, dest)
    if code == 0:
        return (src, dest, True, "converted")
    else:
        return (src, dest, False, f"convert_failed({code})")

def main():
    src_root = Path(SOURCE_ROOT)
    dest_root = Path(DEST_ROOT)

    if not src_root.exists():
        print(f"Source root does not exist: {src_root}", file=sys.stderr)
        sys.exit(2)

    dest_root.mkdir(parents=True, exist_ok=True)

    tasks = []
    total_files = 0
    for dirpath, dirnames, filenames in os.walk(src_root):
        dpath = Path(dirpath)
        for name in filenames:
            src = dpath / name
            ext = src.suffix.lower()
            if ext in VALID_EXTS:
                total_files += 1
                tasks.append(src)
            else:
                unexpected_exts.add(ext)

    log(f"Discovered {total_files} files to convert in {src_root}")
    if unexpected_exts:
        log(f"Note: encountered other file types (not converted): {', '.join(sorted(e or '[noext]' for e in unexpected_exts))}")

    if total_files == 0:
        log("Nothing to do.")
        return

    converted = 0
    skipped = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(process_file, src): src for src in tasks}
        for i, fut in enumerate(as_completed(futures), 1):
            src, dest, ok, note = fut.result()
            # change log interval to every 500
            if ok and note == "converted":
                converted += 1
                if i % 2 == 0:
                    log(f"[{i}/{total_files}] Converted: {src} -> {dest}")
            elif ok and note == "skip_exists":
                skipped += 1
                if i % 2 == 0:
                    log(f"[{i}/{total_files}] Skipped existing: {dest}")
            else:
                failed += 1
                log(f"[{i}/{total_files}] FAILED: {src} -> {dest} ({note})")

    log("----------------------------------------------------------------")
    log(f"Done. Converted: {converted}, Skipped: {skipped}, Failed: {failed}")
    if unexpected_exts:
        log("Other file types encountered (not converted).")
        for e in sorted(unexpected_exts):
            log(f"  * {e or '[no extension]'}")

if __name__ == "__main__":
    main()
