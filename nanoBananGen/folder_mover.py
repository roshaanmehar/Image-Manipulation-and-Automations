#!/usr/bin/env python3
from pathlib import Path
import json
import shutil
import sys

# ===================== CONFIGURE THESE ===================== #
STATE_JSON = Path(r"state.json")

# Map each source directory to its destination directory.
# Edit the paths below to your actual locations.
LOCATIONS = [
    {
        "name": "Location A",
        "source": Path(r"C:\Roshaan\iCloudRenamedConverted"),
        "destination": Path(r"C:\Roshaan\iCloudRenamedConvertedDone"),
    },
    {
        "name": "Location B",
        "source": Path(r"C:\Roshaan\iCloudRenamedConverted\_SelectedCopies"),
        "destination": Path(r"C:\Roshaan\iCloudRenamedConverted\_SelectedCopiesDone"),
    },
]

# Only look at immediate subfolders (True) vs. search recursively (False = still top-level only here).
TOP_LEVEL_ONLY = True

# Set to True first to preview what would be moved without changing anything.
DRY_RUN = False
# =========================================================== #


def load_completed_skus(state_path: Path) -> set[str]:
    if not state_path.is_file():
        sys.exit(f"state.json not found: {state_path}")
    try:
        data = json.loads(state_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        sys.exit(f"Failed to parse JSON: {e}")

    skus = data.get("completed_skus")
    if not isinstance(skus, list):
        sys.exit("state.json does not contain a list at 'completed_skus'.")

    # Normalise to strings and strip whitespace
    return {str(s).strip() for s in skus if str(s).strip()}


def ensure_dir(p: Path):
    try:
        p.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        sys.exit(f"Could not ensure destination directory {p}: {e}")


def folder_matches_sku(folder_name: str, completed: set[str]) -> str | None:
    """
    Match folder names like 'COOCO0022 - 6920220314128'
    Returns the matched SKU if the segment before ' - ' is in the completed set.
    """
    # Fast path: check until first ' - '; if absent, use full name
    if " - " in folder_name:
        left = folder_name.split(" - ", 1)[0].strip()
    else:
        left = folder_name.strip()

    return left if left in completed else None


def move_folder(src: Path, dst_dir: Path):
    target = dst_dir / src.name
    if target.exists():
        # Avoid clobbering: skip if a folder with the same name already exists at destination
        print(f"  SKIP (exists): {target}")
        return

    if DRY_RUN:
        print(f"  DRY-RUN move: {src}  ->  {target}")
        return

    try:
        shutil.move(str(src), str(target))
        print(f"  Moved: {src}  ->  {target}")
    except Exception as e:
        print(f"  ERROR moving {src} -> {target}: {e}")


def process_location(name: str, source: Path, destination: Path, completed: set[str]):
    print(f"\n=== {name} ===")
    if not source.exists():
        print(f"Source missing, skipping: {source}")
        return
    if not source.is_dir():
        print(f"Source is not a directory, skipping: {source}")
        return

    ensure_dir(destination)

    # Iterate subfolders (top-level)
    entries = list(source.iterdir())
    moved_count = 0
    for entry in entries:
        if not entry.is_dir():
            continue

        matched = folder_matches_sku(entry.name, completed)
        if matched:
            print(f"* Match [{matched}]: {entry.name}")
            move_folder(entry, destination)
            moved_count += 1

    if moved_count == 0:
        print("No matching folders found here.")


def main():
    completed = load_completed_skus(STATE_JSON)
    if not completed:
        sys.exit("No completed SKUs found in state.json.")

    print(f"Loaded {len(completed)} completed SKUs from: {STATE_JSON}")
    print(f"DRY_RUN = {DRY_RUN}")
    for loc in LOCATIONS:
        process_location(
            name=loc["name"],
            source=loc["source"],
            destination=loc["destination"],
            completed=completed,
        )

    if DRY_RUN:
        print("\nNothing was moved because DRY_RUN = True. Set DRY_RUN = False to perform the moves.")


if __name__ == "__main__":
    main()
