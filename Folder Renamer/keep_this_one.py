#!/usr/bin/env python3
# keep_this_one.py  — Use from Explorer > Send to
import sys, shutil
from pathlib import Path

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".heic", ".webp", ".tif", ".tiff", ".bmp", ".dng"}

def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p

def unique_dest(p: Path) -> Path:
    if not p.exists(): return p
    stem, ext = p.stem, p.suffix
    i = 2
    while True:
        alt = p.with_name(f"{stem}_v{i}{ext}")
        if not alt.exists():
            return alt
        i += 1

def process_selected_file(selected: Path):
    if not selected.exists() or not selected.is_file():
        print(f"Skip: not a file -> {selected}")
        return
    if selected.suffix.lower() not in IMAGE_EXTS:
        print(f"Skip: not an image -> {selected.name}")
        return

    folder = selected.parent                         # subfolder you’re in
    root = folder.parent                              # assumes structure: <root>\<subfolder>\images...
    # Central trash under the root (single folder, FLAT)
    central_trash = ensure_dir(root / "_CentralTrash")

    # Collect sibling images
    images = [p for p in folder.iterdir() if p.is_file() and p.suffix.lower() in IMAGE_EXTS]
    others = [p for p in images if p != selected]

    if not others:
        print(f"Nothing to move in {folder.name} (only one image or none).")
        return

    # Move others to flat central trash, prefixing with folder name to avoid confusion
    moved = 0
    for p in others:
        dest = unique_dest(central_trash / f"{folder.name}__{p.name}")
        shutil.move(str(p), str(dest))
        moved += 1

    print(f'Kept: {selected.name} | Moved {moved} others -> {central_trash}')

def main():
    if len(sys.argv) < 2:
        print("Usage: keep_this_one.py <selected_file1> [<selected_file2> ...]")
        return
    # If multiple are selected, process each (keeps each selected, removes their siblings)
    for arg in sys.argv[1:]:
        process_selected_file(Path(arg))

if __name__ == "__main__":
    main()
