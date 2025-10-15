#!/usr/bin/env python3
from pathlib import Path
import argparse
import os

ALLOWED = {".jpg", ".jpeg"}

def scan(root: Path):
    findings = []
    for sub in sorted(p for p in root.iterdir() if p.is_dir()):
        files = sorted(p for p in sub.iterdir() if p.is_file())
        non_jpg = [f.name for f in files if f.suffix.lower() not in ALLOWED]
        if len(files) > 1 or non_jpg:
            findings.append((sub.name, len(files), non_jpg))
    return findings

def print_report(findings):
    if not findings:
        print("All subfolders are fine (≤1 file and only JPG/JPEG).")
        return
    name_width = max(len("Subfolder"), max((len(n) for n, _, _ in findings), default=8))
    print(f"{'Subfolder'.ljust(name_width)}  {'Files':>5}  Non-JPG files")
    print(f"{'-'*name_width}  {'-'*5}  {'-'*40}")
    for name, count, non_jpg in findings:
        non = ", ".join(non_jpg) if non_jpg else "-"
        print(f"{name.ljust(name_width)}  {str(count).rjust(5)}  {non}")

def normalise_path(arg_path: str) -> Path:
    # Handles "C:\foo", "./bar", "../baz" and Git Bash "/c/foo".
    p = Path(arg_path)
    if not p.is_absolute():
        if ":" in arg_path and "\\" in arg_path:
            return Path(os.path.normpath(arg_path))
        return (Path.cwd() / p).resolve()
    return Path(os.path.normpath(str(p)))

def main():
    parser = argparse.ArgumentParser(
        description="Report subfolders with >1 file or non-JPG content."
    )
    parser.add_argument(
        "folder", nargs="?", default=".",
        help="Root folder to scan (default: current directory)"
    )
    args = parser.parse_args()

    # Use the argument (or '.') and normalise it to a real Path:
    root = normalise_path(args.folder)
    if not root.exists() or not root.is_dir():
        raise SystemExit(f"Not a folder: {root}")

    findings = scan(root)
    print_report(findings)

if __name__ == "__main__":
    main()
