import os
import sys
import argparse
import base64
import time
import logging
from io import BytesIO
from typing import Iterable, Tuple

# pip install openai pillow python-dotenv
from PIL import Image
from dotenv import load_dotenv
from openai import OpenAI

# ---------- CONFIG (adjust if needed) ----------
ROOT = r"C:\Roshaan\output_images\_MovedFiles"
OUTPUT_DIRNAME = "OPENAI"
# Use OpenAI non-mini image model
OPENAI_IMAGE_MODEL = "gpt-image-1"

# Prompt per your spec (kept very literal and strict)
EDIT_PROMPT = (
    "Return the exact same image of the object but on a pure white background. "
    "Remove any grey colour from the background. "
    "Do not alter in any manner, shape, or way the appearance of the object, "
    "including its colour, proportions, lighting, shadows on the object itself, "
    "textures, labels, or edges. The object must remain exactly as it appears."
)

# Recognised image file extensions
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tiff"}

# Retry/backoff
MAX_ATTEMPTS = 4
BASE_DELAY = 2.0
MAX_DELAY = 16.0
# ----------------------------------------------

load_dotenv()
log = logging.getLogger("openai_bg_clean")

def init_logging(very_verbose: bool):
    level = logging.DEBUG if very_verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

def iter_images(root: str) -> Iterable[Tuple[str, str]]:
    """
    Yields (abs_path, rel_path_from_root) for each image found under root.
    Skips the output subtree (OPENAI) to avoid reprocessing outputs.
    """
    root = os.path.abspath(root)
    output_root = os.path.join(root, OUTPUT_DIRNAME)
    for dirpath, dirnames, filenames in os.walk(root):
        # Skip OPENAI subtree
        if os.path.abspath(dirpath).startswith(os.path.abspath(output_root)):
            continue
        for fn in filenames:
            ext = os.path.splitext(fn)[1].lower()
            if ext in IMAGE_EXTS:
                abs_path = os.path.join(dirpath, fn)
                rel_path = os.path.relpath(abs_path, root)
                yield abs_path, rel_path

def ensure_parent_dir(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)

def save_bytes_to_image(data: bytes, out_path: str, keep_format_ext: str | None):
    """
    Save the returned image bytes. We try to preserve original extension by default.
    If the bytes are PNG (common from API), we transcode via Pillow to the target format.
    """
    # If we don't need to preserve ext, just write raw PNG bytes.
    if not keep_format_ext:
        with open(out_path, "wb") as f:
            f.write(data)
        return

    # Transcode to requested format using Pillow
    with Image.open(BytesIO(data)) as im:
        format_map = {
            ".jpg": "JPEG",
            ".jpeg": "JPEG",
            ".png": "PNG",
            ".webp": "WEBP",
            ".bmp": "BMP",
            ".tiff": "TIFF",
        }
        target_format = format_map.get(keep_format_ext.lower(), "PNG")
        params = {}
        if target_format == "JPEG":
            # Keep quality high; avoid subsampling artefacts
            params.update(dict(quality=95, subsampling=0, optimize=True))
        im.save(out_path, format=target_format, **params)

def openai_edit_image(client: OpenAI, image_bytes: bytes, prompt: str) -> bytes:
    """
    Calls OpenAI Images Edit API (non-mini) with retries, returning PNG bytes.
    """
    attempt = 0
    delay = BASE_DELAY
    last_err = None

    while attempt < MAX_ATTEMPTS:
        attempt += 1
        try:
            # The Images API expects image bytes as base64 via the 'image[]' param.
            # The official Python SDK exposes this through client.images.edits
            # and returns base64 PNG in data[0].b64_json.
            resp = client.images.edits(
                model=OPENAI_IMAGE_MODEL,
                image=[{"image": image_bytes}],
                prompt=prompt,
                size="1024x1024",  # square; keeps things predictable
                # Note: no mask provided — we ask the model in prompt to only change background.
            )
            b64 = resp.data[0].b64_json
            return base64.b64decode(b64)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            last_err = e
            if attempt >= MAX_ATTEMPTS:
                break
            log.warning("Edit failed (attempt %d/%d): %s. Retrying in %.1fs …",
                        attempt, MAX_ATTEMPTS, e, delay)
            time.sleep(delay)
            delay = min(delay * 2, MAX_DELAY)

    raise RuntimeError(f"OpenAI edit failed after {MAX_ATTEMPTS} attempts: {last_err}")

def process(root: str, dry_run: bool, limit: int, overwrite: bool, keep_format: bool, very_verbose: bool):
    init_logging(very_verbose)

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key and not dry_run:
        log.error("OPENAI_API_KEY is not set. Set it in your environment or .env file.")
        sys.exit(1)

    client = OpenAI(api_key=api_key) if not dry_run else None
    root = os.path.abspath(root)
    out_root = os.path.join(root, OUTPUT_DIRNAME)
    os.makedirs(out_root, exist_ok=True)

    log.info("Root: %s", root)
    log.info("Output root: %s", out_root)
    log.info("Model: %s", OPENAI_IMAGE_MODEL)
    log.info("Dry run: %s  | Limit: %s  | Overwrite: %s  | Keep original extension: %s",
             dry_run, (limit if limit else "ALL"), overwrite, keep_format)

    count = 0
    for abs_path, rel_path in iter_images(root):
        if limit and count >= limit:
            break

        src_ext = os.path.splitext(rel_path)[1]
        out_rel = os.path.join(OUTPUT_DIRNAME, rel_path)  # mirror structure inside OPENAI
        out_abs = os.path.join(root, out_rel)

        # Ensure parent directory exists
        ensure_parent_dir(out_abs)

        if (not overwrite) and os.path.exists(out_abs):
            log.info("SKIP (exists): %s -> %s", abs_path, out_abs)
            continue

        log.info("PROCESS: %s -> %s", abs_path, out_abs)

        if dry_run:
            count += 1
            continue

        # Load image bytes
        with open(abs_path, "rb") as f:
            img_bytes = f.read()

        # Call OpenAI edit
        edited_png_bytes = openai_edit_image(client, img_bytes, EDIT_PROMPT)

        # Save (preserve filename & extension by default)
        keep_ext = src_ext if keep_format else None
        save_bytes_to_image(edited_png_bytes, out_abs, keep_ext)
        count += 1

    log.info("Done. Processed %d file(s).", count)

def main():
    parser = argparse.ArgumentParser(description="Batch edit images to white background with OpenAI (non-mini).")
    parser.add_argument("--root", default=ROOT, help="Root folder containing subfolders with images.")
    parser.add_argument("--dry-run", action="store_true", help="Do not call the API; just show what would be done.")
    parser.add_argument("--limit", type=int, default=0, help="Process at most N images (0 = no limit).")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite output files if they already exist.")
    parser.add_argument("--no-keep-format", action="store_true",
                        help="Do not preserve original extension; write the raw PNG returned by the API.")
    parser.add_argument("--very-verbose", action="store_true", help="Debug logging.")
    args = parser.parse_args()

    process(
        root=args.root,
        dry_run=args.dry_run,
        limit=args.limit,
        overwrite=args.overwrite,
        keep_format=not args.no_keep_format,
        very_verbose=args.very_verbose,
    )

if __name__ == "__main__":
    main()
