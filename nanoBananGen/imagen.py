# imagen.py — Cooksmart Nano Banana (Gemini) run-all + robust retries + safe pause/resume
# - Uses google-genai (new SDK)
# - Model: models/gemini-2.5-flash-image
# - Forces 1:1 aspect ratio
# - Upload references once per SKU (reused across 4 prompts)
# - Saves RAW only (exact API output with correct extension) — no transcoding
# - QC: checks for square images only
# - SMTP + ntfy alerts on warnings/errors (errors at highest priority)
#   * Includes header sanitisation for ntfy (ASCII-only headers)
# - Ctrl+C to pause safely; re-run to resume
#   * Resume logic: state tracks COMPLETED SKUs only.
#   * If interrupted mid-SKU, that SKU is NOT marked complete and will be fully re-run.
# - Retries with exponential backoff + jitter for uploads and image generation.
#
# Requirements:
#   pip install google-genai pillow python-dotenv requests
#
# Environment (.env):
#   GEMINI_API_KEY=...
#   SMTP_HOST=...
#   SMTP_PORT=587
#   SMTP_USER=...
#   SMTP_PASS=...
#   SMTP_STARTTLS=true
#   NOTIFY_EMAIL=you@yourdomain.com
#
#   # ntfy: use either NTFY_URL or NTFY_TOPIC (+ optional base)
#   NTFY_URL=https://ntfy.sh/cooksdepo
#   # OR:
#   # NTFY_TOPIC=cooksdepo
#   # NTFY_BASE=https://ntfy.sh
#   # Optional ntfy auth:
#   # NTFY_USERNAME=...
#   # NTFY_PASSWORD=...
#
# Optional:
#   LOG_LEVEL=DEBUG  (default DEBUG)

import os
import sys
import json
import time
import signal
import smtplib
import logging
import argparse
import random
import traceback
import unicodedata
from io import BytesIO
from typing import List, Dict, Tuple, Callable, Any

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.utils import formatdate

from PIL import Image
from dotenv import load_dotenv
import requests
from logging.handlers import RotatingFileHandler

# pip install google-genai pillow python-dotenv requests
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

# ntfy configuration
NTFY_TOPIC = os.getenv("NTFY_TOPIC")
NTFY_BASE = os.getenv("NTFY_BASE", "https://ntfy.sh").rstrip("/")
NTFY_URL = os.getenv("NTFY_URL") or (f"{NTFY_BASE}/{NTFY_TOPIC}" if NTFY_TOPIC else None)
NTFY_USERNAME = os.getenv("NTFY_USERNAME")
NTFY_PASSWORD = os.getenv("NTFY_PASSWORD")

REFERENCE_ROOT = r"C:\Roshaan\OneDrive_1_18-11-2025\master"
OUTPUT_ROOT = os.path.join(os.getcwd(), "output_images")
STATE_FILE = "state.json"          # Tracks COMPLETED SKUs only
ERROR_FILE = "error_log.json"      # Cumulative structured errors
PROMPTS_FILE = "prompts_new.json"
LOG_FILE = "run.log"

MAX_REF_IMAGES = 6

# Default: process ALL SKUs (0 = no limit). Override with --stop-after if you want a cap.
DEFAULT_STOP_AFTER = 0

MODEL = "models/gemini-2.5-flash-image"
RESP_MODALITIES = ["IMAGE"]
ASPECT_RATIO = "1:1"

# Retry policy
RETRY_MAX_ATTEMPTS = int(os.getenv("RETRY_MAX_ATTEMPTS", "4"))
RETRY_BASE_DELAY_S = float(os.getenv("RETRY_BASE_DELAY_S", "2.0"))  # initial backoff
RETRY_MAX_DELAY_S = float(os.getenv("RETRY_MAX_DELAY_S", "20.0"))   # cap backoff

# -------------------- LOGGING --------------------
def init_logging(force_debug: bool = False):
    os.makedirs(OUTPUT_ROOT, exist_ok=True)
    logger = logging.getLogger("imagen")
    logger.propagate = False
    for h in list(logger.handlers):
        logger.removeHandler(h)

    level_name = os.getenv("LOG_LEVEL", "DEBUG")
    level = getattr(logging, level_name.upper(), logging.DEBUG)
    if force_debug:
        level = logging.DEBUG

    logger.setLevel(level)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    _console = logging.StreamHandler(sys.stdout)
    _console.setLevel(level)
    _console.setFormatter(fmt)
    logger.addHandler(_console)

    _file = RotatingFileHandler(LOG_FILE, maxBytes=10_000_000, backupCount=5, encoding="utf-8")
    _file.setLevel(level)
    _file.setFormatter(fmt)
    logger.addHandler(_file)

    logger.debug(f"Logging initialised at level {logging.getLevelName(level)}")
    return logger

logger = init_logging()

# -------------------- SDK CLIENT --------------------
def log_env_summary():
    logger.debug("==== ENV SUMMARY ====")
    logger.debug(f"GEMINI_API_KEY set: {'yes' if bool(GEMINI_API_KEY) else 'NO'}")
    logger.debug(f"SMTP configured: {all([SMTP_HOST, SMTP_USER, SMTP_PASS, NOTIFY_EMAIL])}")
    logger.debug(f"ntfy URL resolved: {NTFY_URL if NTFY_URL else 'None'}")
    if NTFY_URL:
        logger.debug(f"ntfy auth: {'basic' if (NTFY_USERNAME and NTFY_PASSWORD) else 'none'}")
    logger.debug(f"REFERENCE_ROOT: {REFERENCE_ROOT}")
    logger.debug(f"OUTPUT_ROOT: {OUTPUT_ROOT}")
    logger.debug(f"PROMPTS_FILE: {PROMPTS_FILE}")
    logger.debug("=====================")

if not GEMINI_API_KEY:
    logger.error("GEMINI_API_KEY is missing in .env")
    sys.exit(1)

client = genai.Client(api_key=GEMINI_API_KEY)
log_env_summary()

# -------------------- UTIL: TIME --------------------
def now_str() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")

# -------------------- JSON HELPERS --------------------
def load_json(path: str, default):
    logger.debug(f"Loading JSON: {path}")
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            logger.debug(f"Loaded JSON OK: {path}")
            return data
        except Exception as e:
            logger.warning(f"Failed to parse JSON at {path}: {e}. Recreating with default.")
    return default

def save_json(path: str, obj):
    logger.debug(f"Saving JSON: {path}")
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)
    os.replace(tmp, path)
    logger.debug(f"Saved JSON OK: {path}")

def append_error(entry: Dict):
    logger.debug(f"Appending error entry: {entry.get('error_code','unknown')} for {entry.get('product_code','n/a')}:{entry.get('prompt','n/a')}")
    data = load_json(ERROR_FILE, {"errors": []})
    data["errors"].append(entry)
    save_json(ERROR_FILE, data)

# -------------------- NOTIFY (Email + ntfy) --------------------
def _format_html(subject: str, heading: str, details: Dict[str, Any] | None = None, footer: str | None = None) -> str:
    rows = ""
    if details:
        for k, v in details.items():
            rows += f"<tr><td style='padding:4px 8px;font-weight:600'>{k}</td><td style='padding:4px 8px'>{v}</td></tr>"
    footer_html = f"<p style='color:#666'>{footer}</p>" if footer else ""
    return f"""\
<html>
  <body style="font-family:Segoe UI,Arial,sans-serif;line-height:1.45">
    <h2 style="margin:0 0 8px 0">{heading}</h2>
    <p style="margin:0 0 12px 0;color:#333">{subject}</p>
    <table border="0" cellpadding="0" cellspacing="0" style="border-collapse:collapse">{rows}</table>
    {footer_html}
  </body>
</html>
"""

def send_email(subject: str, html_body: str, attachments: list[tuple[str, bytes]] | None = None):
    if not (SMTP_HOST and SMTP_USER and SMTP_PASS and NOTIFY_EMAIL):
        logger.debug("SMTP not fully configured; skipping email send.")
        return
    try:
        logger.debug(f"Preparing email: subject='{subject}' to='{NOTIFY_EMAIL}'")
        msg = MIMEMultipart()
        msg["From"] = SMTP_USER
        msg["To"] = NOTIFY_EMAIL
        msg["Date"] = formatdate(localtime=True)
        msg["Subject"] = subject
        msg.attach(MIMEText(html_body, "html"))

        if attachments:
            for filename, blob in attachments:
                logger.debug(f"Attaching file to email: {filename} ({len(blob)} bytes)")
                part = MIMEApplication(blob, Name=filename)
                part["Content-Disposition"] = f'attachment; filename="{filename}"'
                msg.attach(part)

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as server:
            if SMTP_STARTTLS:
                logger.debug("Starting TLS for SMTP")
                server.starttls()
            logger.debug("Logging in to SMTP")
            server.login(SMTP_USER, SMTP_PASS)
            logger.debug("Sending email")
            server.sendmail(SMTP_USER, NOTIFY_EMAIL, msg.as_string())
        logger.info(f"Email sent: {subject}")
    except Exception as e:
        logger.error(f"Email failed: {e}")

def _sanitize_http_header_value(s: str) -> str:
    """
    HTTP/1.1 headers must be ISO-8859-1. Replace common Unicode punctuation,
    then normalise to ASCII to avoid codec errors in requests.
    """
    if s is None:
        return ""
    replacements = {
        "\u2014": "-",   # em dash —
        "\u2013": "-",   # en dash –
        "\u2212": "-",   # minus −
        "\u00A0": " ",   # non-breaking space
        "\u2018": "'", "\u2019": "'",  # curly single quotes
        "\u201C": '"', "\u201D": '"',  # curly double quotes
    }
    for bad, good in replacements.items():
        s = s.replace(bad, good)
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii", "strict")
    return s.strip()

def send_ntfy(title: str, message: str, priority: int = 5, tags: list[str] | None = None):
    if not NTFY_URL:
        logger.error("ntfy is not configured (NTFY_URL or NTFY_TOPIC missing). Cannot send notification.")
        return

    # Sanitise header values to ASCII-safe strings
    safe_title = _sanitize_http_header_value(title)
    safe_priority = _sanitize_http_header_value(str(priority))
    safe_tags = ",".join(tags) if tags else None
    safe_tags = _sanitize_http_header_value(safe_tags) if safe_tags else None

    headers = {"Title": safe_title, "Priority": safe_priority}
    if safe_tags:
        headers["Tags"] = safe_tags

    auth = (NTFY_USERNAME, NTFY_PASSWORD) if (NTFY_USERNAME and NTFY_PASSWORD) else None

    try:
        hdr_preview = {"Title": headers.get("Title"), "Priority": headers.get("Priority"), "Tags": headers.get("Tags")}
        logger.debug("Posting to ntfy: url=%s, headers=%s", NTFY_URL, hdr_preview)

        # Body can be UTF-8; ntfy handles it
        r = requests.post(NTFY_URL, data=message.encode("utf-8"), headers=headers, auth=auth, timeout=15)
        logger.debug("ntfy response: status=%s, body_snippet='%s'", r.status_code, r.text[:300])
        if r.status_code // 100 != 2:
            logger.warning(f"ntfy responded with status {r.status_code}: {r.text[:500]}")
        else:
            logger.info(f"ntfy sent: {safe_title}")
    except Exception as e:
        logger.error(f"ntfy failed: {e}")

def notify(level: str, title: str, message: str, details: Dict[str, Any] | None = None,
           attach_error_log: bool = False, priority: int = 5, tags: list[str] | None = None):
    """
    Unified notifier: logs, emails, and ntfy.
    - level: 'info' | 'warning' | 'error'
    - All errors are sent to ntfy with priority 5.
    """
    log_line = f"{title} — {message}"
    if level == "error":
        logger.error(log_line)
        priority = 5  # enforce highest priority for any error
    elif level == "warning":
        logger.warning(log_line)
    else:
        logger.info(log_line)

    html = _format_html(subject=message, heading=title, details=details,
                        footer=f"Timestamp: {now_str()}")

    attachments = []
    if attach_error_log and os.path.exists(ERROR_FILE):
        try:
            with open(ERROR_FILE, "rb") as f:
                attachments.append((ERROR_FILE, f.read()))
        except Exception as e:
            logger.debug(f"Could not attach {ERROR_FILE}: {e}")

    # Email always for errors/warnings; info is chatty
    if level in ("warning", "error"):
        send_email(subject=title, html_body=html, attachments=attachments)

    # Always send ntfy for errors; for warnings too for visibility
    if level in ("warning", "error"):
        send_ntfy(title=title, message=message, priority=priority, tags=tags or (["rotating_light"] if level == "error" else ["warning"]))

# -------------------- RETRY HELPER --------------------
def retry_call(func: Callable[..., Any], *args, **kwargs):
    """
    Generic retry with exponential backoff and jitter.
    Raises last exception if all attempts fail.
    """
    attempts = RETRY_MAX_ATTEMPTS
    delay = RETRY_BASE_DELAY_S
    last_exc = None

    logger.debug(f"Retry wrapper start: func={getattr(func,'__name__',str(func))}, attempts={attempts}, base_delay={RETRY_BASE_DELAY_S}, max_delay={RETRY_MAX_DELAY_S}")
    for attempt in range(1, attempts + 1):
        try:
            logger.debug(f"Attempt {attempt}/{attempts} for {getattr(func,'__name__',str(func))}")
            return func(*args, **kwargs)
        except KeyboardInterrupt:
            logger.debug("KeyboardInterrupt caught inside retry_call; re-raising immediately")
            raise
        except Exception as e:
            last_exc = e
            if attempt == attempts:
                break
            jitter = random.uniform(0.7, 1.3)
            sleep_for = min(delay * jitter, RETRY_MAX_DELAY_S)
            logger.warning(f"{getattr(func,'__name__',str(func))} failed (attempt {attempt}/{attempts}): {e.__class__.__name__}: {e}. Retrying in {sleep_for:.1f}s …")
            time.sleep(sleep_for)
            delay = min(delay * 2, RETRY_MAX_DELAY_S)

    logger.debug("All retry attempts exhausted; re-raising last exception")
    raise last_exc

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
def _upload_single_file(path: str):
    logger.debug(f"Uploading single reference file: {path}")
    return client.files.upload(file=path)

def upload_references(folder_path: str) -> List[types.File]:
    logger.debug(f"Uploading references from: {folder_path}")
    files = sorted([f for f in os.listdir(folder_path) if f.lower().endswith((".jpg", ".jpeg"))])
    logger.debug(f"Found {len(files)} JPG/JPEG references: {files}")
    if len(files) == 0:
        raise ValueError(f"No JPG references found in {folder_path}")
    if len(files) > MAX_REF_IMAGES:
        msg = f"Folder '{folder_path}' has {len(files)} images; max allowed is {MAX_REF_IMAGES}"
        logger.warning(msg)
        notify(
            level="warning",
            title="Too many reference images",
            message=msg,
            details={"Folder": folder_path, "Max allowed": MAX_REF_IMAGES, "Found": len(files)},
            attach_error_log=False,
            priority=4,
            tags=["warning"]
        )
        raise ValueError(msg)

    uploaded = []
    for fname in files:
        path = os.path.join(folder_path, fname)
        # Retry each upload individually
        fobj = retry_call(_upload_single_file, path)
        uploaded.append(fobj)
        logger.debug(f"Uploaded: {fname} -> id={getattr(fobj,'name',None) or getattr(fobj,'uri',None)}")
        time.sleep(0.2)
    logger.info(f"Uploaded {len(uploaded)} references from {folder_path}")
    return uploaded

def _generate(model: str, parts, cfg):
    logger.debug(f"Calling models.generate_content(model={model}, parts_len={len(parts)})")
    return client.models.generate_content(
        model=model,
        contents=parts,
        config=cfg,
    )

def generate_one_image(prompt: str, refs: List[types.File]) -> Tuple[bytes, str]:
    logger.debug(f"Generating image with {len(refs)} reference(s); aspect={ASPECT_RATIO}; modalities={RESP_MODALITIES}")
    parts = [prompt]
    parts.extend(refs)

    cfg = types.GenerateContentConfig(
        response_modalities=RESP_MODALITIES,
        image_config=types.ImageConfig(aspect_ratio=ASPECT_RATIO)
    )

    resp = retry_call(_generate, MODEL, parts, cfg)

    # Log some response metadata if present
    cand_count = len(getattr(resp, "candidates", []) or [])
    logger.debug(f"Generation response candidates: {cand_count}")

    for cand_idx, cand in enumerate(getattr(resp, "candidates", [])):
        if not getattr(cand, "content", None):
            logger.debug(f"Candidate {cand_idx} has no content")
            continue
        for part_idx, p in enumerate(getattr(cand.content, "parts", [])):
            if getattr(p, "inline_data", None) and getattr(p.inline_data, "data", None):
                mime = getattr(p.inline_data, "mime_type", "image/png")
                logger.debug(f"Found inline_data in candidate {cand_idx}, part {part_idx}, mime={mime}")
                return p.inline_data.data, mime

    # If we got here, treat as a retryable failure from our caller
    raise RuntimeError("No image bytes returned from API")

# -------------------- STATE (COMPLETED SKUs ONLY) --------------------
def load_completed_skus() -> set:
    state = load_json(STATE_FILE, {"completed_skus": []})
    comps = set(state.get("completed_skus", []))
    logger.debug(f"Loaded completed SKUs: {sorted(list(comps))}")
    return comps

def mark_sku_complete(product_code: str):
    logger.debug(f"Marking SKU complete: {product_code}")
    state = load_json(STATE_FILE, {"completed_skus": []})
    comps = set(state.get("completed_skus", []))
    comps.add(product_code)
    state["completed_skus"] = sorted(comps)
    save_json(STATE_FILE, state)

# -------------------- UTILS --------------------
def find_folder_for_code(code: str) -> str | None:
    logger.debug(f"Searching reference folder for code prefix: {code}")
    for name in os.listdir(REFERENCE_ROOT):
        if name.startswith(code):
            full = os.path.join(REFERENCE_ROOT, name)
            if os.path.isdir(full):
                logger.debug(f"Matched folder '{name}' for code '{code}' -> {full}")
                return full
    logger.debug(f"No reference folder found for code '{code}'")
    return None

def capture_trace() -> str:
    return "".join(traceback.format_exc())

def record_and_notify_error(product_code: str, prompt_key: str, error_code: str, err: Exception, extra: Dict[str, Any] | None = None, pause_on_error: bool = False):
    tb = capture_trace()
    entry = {
        "timestamp": now_str(),
        "product_code": product_code,
        "prompt": prompt_key,
        "error_code": error_code,
        "error_type": err.__class__.__name__,
        "error": str(err),
        "traceback": tb,
    }
    if extra:
        entry["meta"] = extra
    append_error(entry)

    title = f"CRITICAL ERROR — {product_code} / {prompt_key} [{error_code}]"
    message = f"{err.__class__.__name__}: {err}"
    details = {
        "Product": product_code,
        "Prompt": prompt_key,
        "Error code": error_code,
        "Error type": err.__class__.__name__,
        "Message": str(err),
    }
    if extra:
        details.update({f"Meta: {k}": v for k, v in extra.items()})

    notify(
        level="error",
        title=title,
        message=message,
        details=details,
        attach_error_log=True,
        priority=5,
        tags=["rotating_light"]
    )

    if pause_on_error:
        raise err

# -------------------- PER-SKU WORKFLOW --------------------
def process_sku(item: Dict, pause_on_error: bool) -> bool:
    """
    Returns True only if the SKU fully succeeds (all prompts done).
    Any exception/KeyboardInterrupt means the SKU is not marked complete.
    """
    code = item["product_code"]
    logger.info(f"===== START SKU {code} =====")
    logger.debug(f"SKU record: {json.dumps(item, ensure_ascii=False)[:2000]}")

    folder = find_folder_for_code(code)
    if not folder:
        msg = f"Reference folder not found for product_code '{code}' under {REFERENCE_ROOT}"
        logger.warning(msg)
        append_error({
            "timestamp": now_str(),
            "product_code": code,
            "prompt": "n/a",
            "error_code": "reference_folder_missing",
            "error": msg
        })
        notify(
            level="warning",
            title="Missing references",
            message=msg,
            details={"Product": code, "Reference root": REFERENCE_ROOT},
            attach_error_log=True,
            priority=4,
            tags=["warning"]
        )
        logger.info(f"===== END SKU {code} (failed: missing refs) =====")
        return False

    out_dir = os.path.join(OUTPUT_ROOT, os.path.basename(folder))
    os.makedirs(out_dir, exist_ok=True)
    logger.debug(f"Output directory: {out_dir}")

    # Upload references (with retries)
    try:
        refs = upload_references(folder)
    except Exception as e:
        record_and_notify_error(
            product_code=code,
            prompt_key="upload",
            error_code="upload_failed",
            err=e,
            extra={"Folder": folder},
            pause_on_error=pause_on_error,
        )
        logger.info(f"===== END SKU {code} (failed: upload) =====")
        return False

    # Prompts to generate for this SKU
    prompts = {
        "top": item["ecomm_prompts"]["top"],
        "side": item["ecomm_prompts"]["side"],
        "front_45": item["ecomm_prompts"]["front_45"],
        "lifestyle": item["lifestyle_prompt"],
    }
    logger.debug(f"Prompts prepared for {code}: keys={list(prompts.keys())}")

    try:
        for key, prompt in prompts.items():
            logger.info(f"[{code}] Generating '{key}' (len={len(prompt)} chars) using model={MODEL}, aspect={ASPECT_RATIO}")
            try:
                raw_bytes, mime = generate_one_image(prompt, refs)
            except Exception as gen_err:
                # Notify immediately on first failure (including "No image bytes returned from API"),
                # then retry once as before.
                err_msg = str(gen_err)
                err_code = "gen_first_attempt_failed"
                if isinstance(gen_err, RuntimeError) and "No image bytes returned from API" in err_msg:
                    err_code = "no_image_bytes"

                record_and_notify_error(
                    product_code=code,
                    prompt_key=key,
                    error_code=err_code,
                    err=gen_err,
                    extra={"will_retry": True, "model": MODEL, "aspect_ratio": ASPECT_RATIO},
                    pause_on_error=False,  # do not pause here; we retry once
                )

                logger.warning(f"[{code}] '{key}' generation failed once: {gen_err}. Retrying full prompt flow …")
                raw_bytes, mime = generate_one_image(prompt, refs)

            ext = mime_to_ext(mime)
            raw_path = os.path.join(out_dir, f"{code}_{key}_raw{ext}")
            with open(raw_path, "wb") as f:
                f.write(raw_bytes)
            logger.info(f"[{code}] Saved '{key}' to {raw_path} (bytes={len(raw_bytes)}, mime={mime})")

            # QC: must be square (1:1)
            img = Image.open(BytesIO(raw_bytes))
            if not is_square(img):
                msg = f"{code} {key}: image not square ({img.size[0]}x{img.size[1]})"
                logger.warning(msg)
                append_error({
                    "timestamp": now_str(),
                    "product_code": code,
                    "prompt": key,
                    "error_code": "not_square",
                    "error": msg,
                })
                notify(
                    level="warning",
                    title=f"QC warning — {code} / {key} (not square)",
                    message=msg,
                    details={"Product": code, "Prompt": key, "Size": f"{img.size[0]} x {img.size[1]}", "Expected": "1:1"},
                    attach_error_log=True,
                    priority=4,
                    tags=["warning", "ruler"]
                )
                if pause_on_error:
                    raise RuntimeError(msg)

        # All prompts done
        mark_sku_complete(code)
        logger.info(f"SKU complete: {code}")
        logger.info(f"===== END SKU {code} (SUCCESS) =====")
        return True

    except KeyboardInterrupt:
        logger.info("Interrupted mid-SKU; not marking as complete. Re-run to restart this SKU.")
        raise

    except Exception as e:
        record_and_notify_error(
            product_code=code,
            prompt_key="sku_run",
            error_code="sku_run_exception",
            err=e,
            extra=None,
            pause_on_error=pause_on_error,
        )
        logger.info(f"===== END SKU {code} (FAILED) =====")
        return False

# -------------------- CLI --------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pause-on-error", action="store_true", help="Pause (exit) on first error.")
    parser.add_argument("--stop-after", type=int, default=DEFAULT_STOP_AFTER,
                        help="Max SKUs to process this run (0 means ALL).")
    parser.add_argument("--very-verbose", action="store_true", help="Force DEBUG logging for this run.")
    args = parser.parse_args()

    if args.very_verbose:
        # Reinitialise logging at DEBUG if requested
        globals()["logger"] = init_logging(force_debug=True)
        log_env_summary()

    # Convert Ctrl+C into KeyboardInterrupt immediately
    signal.signal(signal.SIGINT, lambda sig, frame: (_ for _ in ()).throw(KeyboardInterrupt()))

    logger.info("=== Run starting ===")
    # Load prompts
    data = load_json(PROMPTS_FILE, [])
    if not data:
        msg = f"No prompts found in {PROMPTS_FILE}"
        logger.error(msg)
        append_error({
            "timestamp": now_str(),
            "product_code": "n/a",
            "prompt": "startup",
            "error_code": "prompts_missing",
            "error": msg
        })
        notify(
            level="error",
            title="Startup error — prompts.json missing/empty",
            message=msg,
            details={"File": PROMPTS_FILE, "Working dir": os.getcwd()},
            attach_error_log=True,
            priority=5,
            tags=["rotating_light"]
        )
        return

    # Resume: skip SKUs already completed in previous runs
    completed = load_completed_skus()
    pending_items = [item for item in data if item.get("product_code") not in completed]
    logger.info(f"Pending SKUs: {len(pending_items)} (completed already: {len(completed)})")

    if not pending_items:
        logger.info("Nothing to do. All SKUs in prompts.json are marked complete.")
        return

    # Respect stop-after limit (0 => all)
    if args.stop_after and args.stop_after > 0:
        logger.info(f"Applying stop-after cap: {args.stop_after}")
        pending_items = pending_items[: args.stop_after]

    processed_success = 0
    total = len(pending_items)
    logger.info(f"Processing {total} SKU(s) this run")

    for idx, item in enumerate(pending_items, start=1):
        code = item.get("product_code", "UNKNOWN")
        logger.info(f"--- [{idx}/{total}] Begin {code} ---")
        try:
            ok = process_sku(item, pause_on_error=args.pause_on_error)
            if ok:
                processed_success += 1
        except KeyboardInterrupt:
            logger.info("Paused by user; exiting gracefully. Progress saved for completed SKUs only.")
            break
        except Exception as e:
            record_and_notify_error(
                product_code=code,
                prompt_key="run_loop",
                error_code="fatal_run_stop",
                err=e,
                extra=None,
                pause_on_error=False,
            )
            break
        finally:
            logger.info(f"--- [{idx}/{total}] End {code} ---")

    logger.info(f"Run complete. Successful SKUs this run: {processed_success}/{total}")
    logger.info("=== Run finished ===")

if __name__ == "__main__":
    main()
