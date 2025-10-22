# imagen.py — Cooksmart Nano Banana (Gemini) 3-SKU test (no transcoding; safe pause + resume)
# - Uses google-genai (new SDK)
# - Model: models/gemini-2.5-flash-image
# - Forces 1:1 aspect ratio
# - Upload references once per SKU (reused across 4 prompts)
# - Saves RAW (exact API output with correct extension), no transcoding
# - QC: only checks for square images (white background check removed)
# - SMTP alerts on errors/warnings
# - Ctrl+C to pause safely; re-run to resume
#   * Resume logic: state tracks COMPLETED SKUs only.
#   * If interrupted mid-SKU, that SKU is NOT marked complete and will be fully re-run.

import os
import sys
import json
import time
import signal
import smtplib
import logging
import argparse
from io import BytesIO
from typing import List, Dict, Tuple

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.utils import formatdate

from PIL import Image
from dotenv import load_dotenv

# pip install google-genai pillow python-dotenv
from google import genai
from google.genai import types

# -------------------- SETTINGS --------------------
load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
SMTP_STARTTLS = os.getenv("SMTP_STARTTLS", "true").lower() == "true"
NOTIFY_EMAIL = os.getenv("NOTIFY_EMAIL")

REFERENCE_ROOT = r"C:\Roshaan\iCloudRenamedConverted"
OUTPUT_ROOT = os.path.join(os.getcwd(), "output_images")
STATE_FILE = "state.json"          # Tracks COMPLETED SKUs only
ERROR_FILE = "error_log.json"
PROMPTS_FILE = "prompts.json"

MAX_REF_IMAGES = 6
DEFAULT_STOP_AFTER = 3  # process only 3 SKUs for testing

MODEL = "models/gemini-2.5-flash-image"
RESP_MODALITIES = ["IMAGE"]
ASPECT_RATIO = "1:1"

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# -------------------- SDK CLIENT --------------------
if not GEMINI_API_KEY:
    logging.error("GEMINI_API_KEY is missing in .env")
    sys.exit(1)

client = genai.Client(api_key=GEMINI_API_KEY)

# -------------------- EMAIL --------------------
def send_email(subject: str, html_body: str, attachment_bytes: bytes = None, filename: str = None):
    if not (SMTP_HOST and SMTP_USER and SMTP_PASS and NOTIFY_EMAIL):
        logging.warning("SMTP not fully configured; skipping email.")
        return
    try:
        msg = MIMEMultipart()
        msg["From"] = SMTP_USER
        msg["To"] = NOTIFY_EMAIL
        msg["Date"] = formatdate(localtime=True)
        msg["Subject"] = subject
        msg.attach(MIMEText(html_body, "html"))

        if attachment_bytes and filename:
            part = MIMEApplication(attachment_bytes, Name=filename)
            part["Content-Disposition"] = f'attachment; filename="{filename}"'
            msg.attach(part)

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            if SMTP_STARTTLS:
                server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_USER, NOTIFY_EMAIL, msg.as_string())
        logging.info(f"Email sent: {subject}")
    except Exception as e:
        logging.error(f"Email failed: {e}")

# -------------------- JSON HELPERS --------------------
def load_json(path: str, default):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default

def save_json(path: str, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)

def append_error(entry: Dict):
    data = load_json(ERROR_FILE, {"errors": []})
    data["errors"].append(entry)
    save_json(ERROR_FILE, data)

# -------------------- QC HELPERS --------------------
def is_square(img: Image.Image) -> bool:
    w, h = img.size
    return w == h

def mime_to_ext(mime: str) -> str:
    if not mime:
        return ".png"
    mime = mime.lower()
    if "webp" in mime:
        return ".webp"
    if "jpeg" in mime or "jpg" in mime:
        return ".jpg"
    if "png" in mime:
        return ".png"
    return ".png"

# -------------------- GEMINI INTEGRATION --------------------
def upload_references(folder_path: str) -> List[types.File]:
    jpgs = [f for f in os.listdir(folder_path) if f.lower().endswith((".jpg", ".jpeg"))]
    if len(jpgs) == 0:
        raise ValueError(f"No JPG references found in {folder_path}")
    if len(jpgs) > MAX_REF_IMAGES:
        send_email("Too many reference images",
                   f"<p>Folder <b>{folder_path}</b> has more than {MAX_REF_IMAGES} images.</p>")
        raise ValueError(f"Too many images in {folder_path}")

    uploaded = []
    for fname in jpgs:
        path = os.path.join(folder_path, fname)
        fobj = client.files.upload(file=path)
        uploaded.append(fobj)
        time.sleep(0.3)
    return uploaded

def generate_one_image(prompt: str, refs: List[types.File]) -> Tuple[bytes, str]:
    parts = [prompt]
    parts.extend(refs)

    cfg = types.GenerateContentConfig(
        response_modalities=RESP_MODALITIES,
        image_config=types.ImageConfig(aspect_ratio=ASPECT_RATIO)
    )

    resp = client.models.generate_content(
        model=MODEL,
        contents=parts,
        config=cfg,
    )

    # Safely iterate only valid candidates
    for cand in getattr(resp, "candidates", []):
        if not getattr(cand, "content", None):
            continue
        for p in getattr(cand.content, "parts", []):
            if getattr(p, "inline_data", None) and getattr(p.inline_data, "data", None):
                mime = getattr(p.inline_data, "mime_type", "image/png")
                return p.inline_data.data, mime

    raise RuntimeError("No image bytes returned from API")

# -------------------- STATE (COMPLETED SKUs ONLY) --------------------
def load_completed_skus() -> set:
    state = load_json(STATE_FILE, {"completed_skus": []})
    return set(state.get("completed_skus", []))

def mark_sku_complete(product_code: str):
    state = load_json(STATE_FILE, {"completed_skus": []})
    comps = set(state.get("completed_skus", []))
    comps.add(product_code)
    state["completed_skus"] = sorted(comps)
    save_json(STATE_FILE, state)

# -------------------- UTILS --------------------
def find_folder_for_code(code: str) -> str:
    for name in os.listdir(REFERENCE_ROOT):
        if name.startswith(code):
            full = os.path.join(REFERENCE_ROOT, name)
            if os.path.isdir(full):
                return full
    return None

# -------------------- PER-SKU WORKFLOW --------------------
def process_sku(item: Dict, pause_on_error: bool) -> bool:
    """
    Returns True only if the SKU fully succeeds (all prompts done).
    Any exception/KeyboardInterrupt means the SKU is not marked complete.
    """
    code = item["product_code"]
    folder = find_folder_for_code(code)
    if not folder:
        logging.warning(f"Folder missing for {code}")
        append_error({
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "product_code": code,
            "prompt": "n/a",
            "error": "reference_folder_missing"
        })
        return False

    out_dir = os.path.join(OUTPUT_ROOT, os.path.basename(folder))
    os.makedirs(out_dir, exist_ok=True)
    logging.info(f"Processing {code} ...")

    try:
        refs = upload_references(folder)
    except Exception as e:
        logging.error(f"Upload failed for {code}: {e}")
        append_error({
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "product_code": code,
            "prompt": "upload",
            "error": str(e),
        })
        send_email(f"Upload failed for {code}", f"<p>{e}</p>")
        if pause_on_error:
            raise
        return False

    # Prompts to generate for this SKU
    prompts = {
        "top": item["ecomm_prompts"]["top"],
        "side": item["ecomm_prompts"]["side"],
        "front_45": item["ecomm_prompts"]["front_45"],
        "lifestyle": item["lifestyle_prompt"],
    }

    try:
        for key, prompt in prompts.items():
            # Generate and save RAW image
            raw_bytes, mime = generate_one_image(prompt, refs)

            ext = mime_to_ext(mime)
            raw_path = os.path.join(out_dir, f"{code}_{key}_raw{ext}")
            with open(raw_path, "wb") as f:
                f.write(raw_bytes)

            # QC: must be square (1:1)
            img = Image.open(BytesIO(raw_bytes))
            if not is_square(img):
                msg = f"{code} {key}: image not square ({img.size[0]}x{img.size[1]})"
                logging.warning(msg)
                append_error({
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "product_code": code,
                    "prompt": key,
                    "error": "not_square",
                })
                send_email(f"QC warning: {code} {key}", f"<p>{msg}</p>")
                if pause_on_error:
                    raise RuntimeError(msg)

            logging.info(f"Saved {key} for {code}")

        # If we reached here, all prompts finished for this SKU
        mark_sku_complete(code)
        logging.info(f"SKU complete: {code}")
        return True

    except KeyboardInterrupt:
        logging.info("Interrupted mid-SKU; not marking as complete. You can rerun to restart this SKU.")
        # Let the interrupt bubble up so main can stop gracefully
        raise

    except Exception as e:
        logging.error(f"Error for {code}: {e}")
        append_error({
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "product_code": code,
            "prompt": "sku_run",
            "error": str(e),
        })
        send_email(f"Error: {code}", f"<p>{e}</p>")
        if pause_on_error:
            raise
        return False

# -------------------- CLI --------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pause-on-error", action="store_true", help="Pause (exit) on first error.")
    parser.add_argument("--stop-after", type=int, default=DEFAULT_STOP_AFTER, help="Max SKUs to process this run.")
    args = parser.parse_args()

    os.makedirs(OUTPUT_ROOT, exist_ok=True)

    # Convert Ctrl+C into KeyboardInterrupt immediately
    signal.signal(signal.SIGINT, lambda sig, frame: (_ for _ in ()).throw(KeyboardInterrupt()))

    # Load prompts
    data = load_json(PROMPTS_FILE, [])
    if not data:
        logging.error(f"No prompts found in {PROMPTS_FILE}")
        return

    # Resume: skip SKUs already completed in previous runs
    completed = load_completed_skus()
    pending_items = [item for item in data if item.get("product_code") not in completed]

    if not pending_items:
        logging.info("Nothing to do. All SKUs in prompts.json are marked complete.")
        return

    # Respect stop-after limit
    pending_items = pending_items[: args.stop_after]

    processed_success = 0
    total = len(pending_items)

    for item in pending_items:
        code = item.get("product_code", "UNKNOWN")
        try:
            ok = process_sku(item, pause_on_error=args.pause_on_error)
            if ok:
                processed_success += 1
        except KeyboardInterrupt:
            logging.info("Paused by user; exiting gracefully. Progress saved for completed SKUs only.")
            break
        except Exception as e:
            logging.error(f"Stopped on {code}: {e}")
            break

    logging.info(f"Run complete. Successful SKUs this run: {processed_success}/{total}")

if __name__ == "__main__":
    main()
