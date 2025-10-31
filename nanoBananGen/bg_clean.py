#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Process PNGs under C:\Roshaan\output_images\_MovedFiles\, send each to OpenAI gpt-image-1
to produce a 1024x1024 PNG with a pure white (#FFFFFF) background, without changing the
product’s appearance. Saves results under ...\_MovedFiles\OPENAI\<same-subfolders>\ with
the same filename (overwriting if it exists).

Usage (PowerShell / CMD):
    python process_openai_white_bg.py
    python process_openai_white_bg.py --dry-run
    python process_openai_white_bg.py --test   (only first 5 subfolders)
"""

import base64
import os
import sys
import time
import json
import argparse
from typing import Optional, Tuple
import requests

try:
    from openai import OpenAI
    _HAS_OPENAI_SDK = True
except Exception:
    _HAS_OPENAI_SDK = False

try:
    from tqdm import tqdm
    _HAS_TQDM = True
except Exception:
    _HAS_TQDM = False


# -------- Configuration --------
ROOT_DIR = r"C:\Roshaan\output_images\_MovedFiles"
OUTPUT_ROOT = os.path.join(ROOT_DIR, "OPENAI")
MODEL = "gpt-image-1"
SIZE = "1024x1024"
ALLOWED_EXTS = {".png"}
MAX_RETRIES = 6
INITIAL_DELAY = 1.0
TIMEOUT = 120

PROMPT = (
    "Return a single 1024x1024 PNG. Replace only the background with pure white (#FFFFFF). "
    "Remove any grey from the background. Do not in any way alter the product or its appearance, "
    "including colours, lighting, geometry, texture, reflections, shadows on the product, or any details. "
    "Keep the product exactly as-is; change the background only."
)


# -------- Helpers --------
def is_inside_output_dir(path: str) -> bool:
    out = os.path.abspath(OUTPUT_ROOT)
    p = os.path.abspath(path)
    return p.startswith(out)


def iter_source_pngs(root_dir: str, test_mode: bool = False):
    """
    Yield all PNGs in root_dir. If test_mode=True, restrict to first 5 unique subfolders.
    """
    counted_subfolders = set()
    allowed_subfolders = set()

    for dirpath, dirnames, filenames in os.walk(root_dir):
        if is_inside_output_dir(dirpath):
            continue

        # Determine top-level subfolder relative to root
        rel = os.path.relpath(dirpath, root_dir)
        parts = rel.split(os.sep)
        top = parts[0] if parts[0] != "." else "."

        if test_mode:
            if top not in counted_subfolders and top != ".":
                counted_subfolders.add(top)
                if len(counted_subfolders) > 5:
                    continue
            if len(counted_subfolders) <= 5:
                allowed_subfolders = counted_subfolders.copy()
            else:
                continue

            if top != "." and top not in allowed_subfolders:
                continue

        for name in filenames:
            ext = os.path.splitext(name)[1].lower()
            if ext in ALLOWED_EXTS:
                yield os.path.join(dirpath, name)


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def dest_path_for(src_path: str) -> Tuple[str, str]:
    rel_dir = os.path.relpath(os.path.dirname(src_path), ROOT_DIR)
    if rel_dir == ".":
        rel_dir = ""
    dest_dir = os.path.join(OUTPUT_ROOT, rel_dir)
    ensure_dir(dest_dir)
    dest_file = os.path.join(dest_dir, os.path.basename(src_path))
    return dest_dir, dest_file


def exponential_backoff_attempts(max_tries: int = MAX_RETRIES, initial_delay: float = INITIAL_DELAY):
    delay = initial_delay
    for attempt in range(1, max_tries + 1):
        yield attempt, delay
        delay *= 2.0


def decode_and_save_png(b64_data: str, dest_file: str):
    raw = base64.b64decode(b64_data)
    with open(dest_file, "wb") as f:
        f.write(raw)


# -------- OpenAI Calls --------
def call_openai_via_sdk(image_path: str) -> Optional[str]:
    if not _HAS_OPENAI_SDK:
        return None

    try:
        client = OpenAI(timeout=TIMEOUT)
        with open(image_path, "rb") as f:
            try:
                resp = client.images.edits(
                    model=MODEL,
                    image=f,
                    prompt=PROMPT,
                    size=SIZE,
                    n=1,
                    response_format="b64_json",
                )
            except TypeError:
                f.seek(0)
                resp = client.images.edits(
                    model=MODEL,
                    image=[f],
                    prompt=PROMPT,
                    size=SIZE,
                    n=1,
                    response_format="b64_json",
                )

        data = getattr(resp, "data", None) or resp.get("data")
        if data and len(data) > 0:
            b64 = data[0].get("b64_json")
            return b64
        return None
    except Exception:
        return None


def call_openai_via_http(image_path: str, api_key: str) -> Optional[str]:
    url = "https://api.openai.com/v1/images/edits"
    headers = {"Authorization": f"Bearer {api_key}"}
    files = {"image": (os.path.basename(image_path), open(image_path, "rb"), "image/png")}
    data = {
        "model": MODEL,
        "prompt": PROMPT,
        "size": SIZE,
        "n": 1,
        "background": "white",
        "response_format": "b64_json",
    }

    try:
        r = requests.post(url, headers=headers, files=files, data=data, timeout=TIMEOUT)
        if r.status_code == 200:
            j = r.json()
            if "data" in j and j["data"]:
                return j["data"][0].get("b64_json")
        return None
    finally:
        try:
            files["image"][1].close()
        except Exception:
            pass


def process_one_image(image_path: str, dest_file: str, api_key: str, dry_run: bool = False) -> bool:
    if dry_run:
        print(f"[DRY] Would process: {image_path} -> {dest_file}")
        return True

    for attempt, delay in exponential_backoff_attempts():
        b64 = None
        if _HAS_OPENAI_SDK:
            b64 = call_openai_via_sdk(image_path)
        if not b64:
            b64 = call_openai_via_http(image_path, api_key)
        if b64:
            decode_and_save_png(b64, dest_file)
            return True
        if attempt < MAX_RETRIES:
            time.sleep(delay)
        else:
            return False
    return False


def main():
    parser = argparse.ArgumentParser(description="OpenAI white-background normaliser for PNGs.")
    parser.add_argument("--dry-run", action="store_true", help="Don’t call API or write files; just print actions.")
    parser.add_argument("--test", action="store_true", help="Process only the first 5 subfolders.")
    args = parser.parse_args()

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key and not _HAS_OPENAI_SDK:
        print("ERROR: OPENAI_API_KEY not set, and OpenAI SDK unavailable.", file=sys.stderr)
        sys.exit(1)

    print(f"Scanning: {ROOT_DIR}")
    pngs = list(iter_source_pngs(ROOT_DIR, test_mode=args.test))

    if args.test:
        subs = {os.path.relpath(os.path.dirname(p), ROOT_DIR).split(os.sep)[0] for p in pngs}
        print(f"[TEST MODE] Will process up to 5 subfolders: {sorted(subs)}")

    if not pngs:
        print("No PNG files found.")
        return

    iterable = tqdm(pngs, desc="Processing", unit="img") if _HAS_TQDM else pngs

    success, fail = 0, 0
    for src in iterable:
        try:
            _, dest = dest_path_for(src)
            ok = process_one_image(src, dest, api_key or "", dry_run=args.dry_run)
            if ok:
                success += 1
            else:
                fail += 1
            if _HAS_TQDM:
                iterable.set_postfix_str(f"ok={success} fail={fail}", refresh=False)
            else:
                print(f"{'OK' if ok else 'FAIL'}: {src}")
        except KeyboardInterrupt:
            print("\nInterrupted by user.")
            break
        except Exception as e:
            fail += 1
            print(f"ERROR processing {src}: {e}", file=sys.stderr)

    print(f"\nDone. Success: {success}, Failed: {fail}")


if __name__ == "__main__":
    main()
