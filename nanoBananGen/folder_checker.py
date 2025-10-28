#!/usr/bin/env python3
import argparse
import json
import os
import re
import shutil
from pathlib import Path

FOLDER_NAME_PATTERN = re.compile(r"^\s*([A-Za-z0-9_-]+)\s*-\s*.+$")  # captures ITEMCODE before " - "

def get_subfolder_names(path: Path) -> set[str]:
    """Return names of immediate subfolders within path."""
    if not path.exists():
        raise FileNotFoundError(f"Directory not found: {path}")
    if not path.is_dir():
        raise NotADirectoryError(f"Not a directory: {path}")
    return {p.name for p in path.iterdir() if p.is_dir()}

def extract_itemcode(folder_name: str) -> str | None:
    """
    Given 'ITEMCODE - BARCODE', return 'ITEMCODE'.
    Returns None if pattern doesn't match.
    """
    m = FOLDER_NAME_PATTERN.match(folder_name)
    return m.group(1) if m else None

def load_state(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if "completed_skus" not in data or not isinstance(data["completed_skus"], list):
        raise ValueError("state.json must contain a key 'completed_skus' with a list value.")
    return data

def save_state(path: Path, data: dict):
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")

def move_folder(src: Path, dst_dir: Path) -> Path:
    """
    Move src folder into dst_dir. If a folder with the same name exists,
    append a numeric suffix.
    Returns the final destination path used.
    """
    dst_dir.mkdir(parents=True, exist_ok=True)
    target = dst_dir / src.name
    if not target.exists():
        shutil.move(str(src), str(target))
        return target

    # Resolve collision by suffixing "-1", "-2", ...
    base = src.name
    n = 1
    while True:
        candidate = dst_dir / f"{base}-{n}"
        if not candidate.exists():
            shutil.move(str(src), str(candidate))
            return candidate
        n += 1

def main():
    parser = argparse.ArgumentParser(
        description="Sync helper: update state.json and move subfolders missing from folder_2."
    )
    parser.add_argument("--folder-1", required=True, type=Path, help="Source folder (authoritative).")
    parser.add_argument("--folder-2", required=True, type=Path, help="Destination folder to compare against.")
    parser.add_argument("--state", required=True, type=Path, help="Path to state.json.")
    parser.add_argument("--state-out", type=Path, default=None,
                        help="Path to write updated JSON (default: alongside state.json as state_updated.json).")
    parser.add_argument("--move-to", required=True, type=Path,
                        help="Directory to move folders that exist in folder_1 but not in folder_2.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would happen without changing files.")
    args = parser.parse_args()

    folder_1 = args.folder_1.resolve()
    folder_2 = args.folder_2.resolve()
    move_dest = args.move_to.resolve()
    state_path = args.state.resolve()
    state_out = args.state_out.resolve() if args.state_out else state_path.with_name("state_updated.json")

    # 1) Read folders
    sub_1 = get_subfolder_names(folder_1)
    sub_2 = get_subfolder_names(folder_2)

    missing_in_2 = sorted(sub_1 - sub_2)  # folder names present in folder_1, absent in folder_2

    # 2) From those missing names, extract itemcodes
    missing_itemcodes = set()
    unparsable = []
    for name in missing_in_2:
        code = extract_itemcode(name)
        if code:
            missing_itemcodes.add(code)
        else:
            unparsable.append(name)

    # 3) Load and update state.json
    state = load_state(state_path)
    before = list(state["completed_skus"])
    before_set = set(before)

    # Remove any code that’s not present as a subfolder in folder_2 (inferred from missing_in_2 itemcodes)
    updated = [sku for sku in before if sku not in missing_itemcodes]

    state_updated = dict(state)
    state_updated["completed_skus"] = updated

    # 4) Write updated JSON
    if args.dry_run:
        print("DRY RUN: would write updated state to:", state_out)
    else:
        save_state(state_out, state_updated)

    # 5) Move missing folders to new location
    moved = []
    errors = []

    if args.dry_run:
        print("\nDRY RUN: the following folders would be moved to", move_dest)
        for name in missing_in_2:
            print("  -", (folder_1 / name).as_posix(), "→", (move_dest / name).as_posix())
    else:
        for name in missing_in_2:
            src = folder_1 / name
            try:
                final_dst = move_folder(src, move_dest)
                moved.append((src, final_dst))
            except Exception as e:
                errors.append((src, str(e)))

    # 6) Report
    print("\n=== Summary ===")
    print(f"Folder 1: {folder_1}")
    print(f"Folder 2: {folder_2}")
    print(f"Missing in folder_2 (count {len(missing_in_2)}):")
    for n in missing_in_2:
        print("  -", n)

    if unparsable:
        print("\nWarning: the following folder names did not match 'ITEMCODE - BARCODE' pattern:")
        for n in unparsable:
            print("  -", n)

    removed_from_state = sorted(before_set - set(updated))
    print("\nState.json:")
    print(f"  Input path:  {state_path}")
    print(f"  Output path: {state_out}")
    print(f"  Completed SKUs before: {len(before)}")
    print(f"  Completed SKUs after:  {len(updated)}")
    if removed_from_state:
        print("  Removed SKUs:")
        for sku in removed_from_state:
            print("    -", sku)
    else:
        print("  No SKUs removed.")

    if args.dry_run:
        print("\nNo folders moved (dry run).")
    else:
        print(f"\nMoved folders (count {len(moved)}):")
        for src, dst in moved:
            print("  -", src.name, "→", dst)
        if errors:
            print("\nErrors while moving:")
            for src, msg in errors:
                print("  -", src, ":", msg)

if __name__ == "__main__":
    main()
