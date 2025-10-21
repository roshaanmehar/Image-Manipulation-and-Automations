"""
main.py — Cooksmart Nano Banana Image Generator (3-SKU Test)
Author: ChatGPT (for Selma)
"""

import os
import json
import time
import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.utils import formatdate
from io import BytesIO
from PIL import Image
from dotenv import load_dotenv
import google.generativeai as genai


# -------------------- CONFIG --------------------
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
STATE_FILE = "state.json"
LOG_FILE = "run_log.json"
ERROR_FILE = "error_log.json"
PROMPTS_FILE = "prompts.json"

MAX_REF_IMAGES = 6
PAUSE_ON_ERROR = True
MODEL = "models/gemini-2.0-flash-preview-image-generation"

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)


# -------------------- EMAIL --------------------
def send_email(subject: str, html_body: str, attachment=None, filename=None):
    """Send an alert email via Outlook SMTP."""
    try:
        msg = MIMEMultipart()
        msg['From'] = SMTP_USER
        msg['To'] = NOTIFY_EMAIL
        msg['Date'] = formatdate(localtime=True)
        msg['Subject'] = subject
        msg.attach(MIMEText(html_body, 'html'))

        if attachment and filename:
            part = MIMEApplication(attachment, Name=filename)
            part['Content-Disposition'] = f'attachment; filename="{filename}"'
            msg.attach(part)

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            if SMTP_STARTTLS:
                server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_USER, NOTIFY_EMAIL, msg.as_string())
        logging.info(f"Email sent: {subject}")
    except Exception as e:
        logging.error(f"Email send failed: {e}")


# -------------------- UTILITIES --------------------
def save_json(filename, data):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def load_json(filename):
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def check_background_white(image: Image.Image, tolerance=15):
    """Check that image background is near pure white."""
    pixels = image.convert("RGB").getdata()
    total = len(pixels)
    whiteish = sum(
        1 for p in pixels if all(c > (255 - tolerance) for c in p)
    )
    return whiteish / total > 0.95


def transcode_image(raw_bytes: bytes, dest_path: str):
    """Upscale and convert image to WebP for comparison."""
    img = Image.open(BytesIO(raw_bytes)).convert("RGB")
    w, h = img.size
    if w != 2048 or h != 2048:
        img = img.resize((2048, 2048), Image.LANCZOS)
    img.save(dest_path, "WEBP", quality=95)
    return dest_path


# -------------------- GEMINI API --------------------
genai.configure(api_key=GEMINI_API_KEY)


def upload_reference_images(folder_path):
    """Upload ≤6 JPGs and return list of file IDs."""
    file_ids = []
    files = [
        f for f in os.listdir(folder_path)
        if f.lower().endswith(".jpg") or f.lower().endswith(".jpeg")
    ]
    if len(files) > MAX_REF_IMAGES:
        send_email(
            "Too many reference images",
            f"<p>Folder {folder_path} has more than {MAX_REF_IMAGES} images.</p>"
        )
        raise ValueError(f"Too many images in {folder_path}")
    for fname in files:
        path = os.path.join(folder_path, fname)
        file_obj = genai.upload_file(path)
        file_ids.append(file_obj.name)
        time.sleep(1)  # gentle rate
    return file_ids


def generate_image(prompt: str, file_ids):
    """Generate a single image using Nano Banana."""
    response = genai.images.generate(
        model=MODEL,
        prompt=prompt,
        files=file_ids
    )
    if not response or not getattr(response, "generated_images", None):
        raise RuntimeError("No image returned from API")
    return response.generated_images[0].image_bytes


# -------------------- MAIN LOGIC --------------------
def log_error(code, prompt_type, message):
    entry = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "product_code": code,
        "prompt": prompt_type,
        "error": message
    }
    errors = load_json(ERROR_FILE)
    errors.setdefault("errors", []).append(entry)
    save_json(ERROR_FILE, errors)


def process_sku(item, state):
    code = item["product_code"]
    folder_name = next((f for f in os.listdir(REFERENCE_ROOT) if f.startswith(code)), None)
    if not folder_name:
        logging.warning(f"No folder found for {code}")
        return

    ref_folder = os.path.join(REFERENCE_ROOT, folder_name)
    out_folder = os.path.join(OUTPUT_ROOT, folder_name)
    os.makedirs(out_folder, exist_ok=True)
    logging.info(f"Processing {code}...")

    try:
        file_ids = upload_reference_images(ref_folder)
    except Exception as e:
        logging.error(f"Reference upload failed for {code}: {e}")
        log_error(code, "upload", str(e))
        if PAUSE_ON_ERROR:
            raise

    prompts = {
        "top": item["ecomm_prompts"]["top"],
        "side": item["ecomm_prompts"]["side"],
        "front_45": item["ecomm_prompts"]["front_45"],
        "lifestyle": item["lifestyle_prompt"]
    }

    for key, prompt in prompts.items():
        try:
            img_bytes = generate_image(prompt, file_ids)

            raw_path = os.path.join(out_folder, f"{code}_{key}_raw.png")
            with open(raw_path, "wb") as f:
                f.write(img_bytes)

            # Transcode for testing
            transcoded_path = os.path.join(out_folder, f"{code}_{key}_transcoded.webp")
            transcode_image(img_bytes, transcoded_path)

            # QC
            img = Image.open(BytesIO(img_bytes))
            w, h = img.size
            if w != h:
                raise ValueError(f"Image not square: {w}x{h}")
            if key != "lifestyle" and not check_background_white(img):
                raise ValueError(f"Background not pure white for {key}")

            logging.info(f"Saved {key} for {code}")
            state[code] = state.get(code, []) + [key]
            save_json(STATE_FILE, state)

        except Exception as e:
            logging.error(f"Error generating {key} for {code}: {e}")
            log_error(code, key, str(e))
            send_email(
                f"Error generating {key} for {code}",
                f"<p>{e}</p>"
            )
            if PAUSE_ON_ERROR:
                raise


def main():
    state = load_json(STATE_FILE)
    data = json.load(open(PROMPTS_FILE, "r", encoding="utf-8"))[:3]  # limit to 3 SKUs
    total = len(data)
    processed = 0

    for item in data:
        code = item["product_code"]
        done = state.get(code, [])
        if len(done) == 4:
            continue
        try:
            process_sku(item, state)
            processed += 1
        except Exception as e:
            logging.error(f"Stopped on {code}: {e}")
            if PAUSE_ON_ERROR:
                break
            continue

    logging.info(f"Completed {processed} SKUs.")


if __name__ == "__main__":
    main()
