import os
import sys
import base64
import logging
import time
import random
from typing import List, Tuple
from pathlib import Path

import requests
from dotenv import load_dotenv
from openai import OpenAI

# -------------------- CONFIG --------------------
ROOT = Path(r"C:\Roshaan\output_images\_MovedFiles")
OUTPUT_SUBDIR = "AIOPEN"
ACCEPTED_EXT = {".png"}
BATCH_LIMIT = 4               # per your structure
FORCE_SIZE = "1024x1024"      # fixed by requirement

PROMPT = (
    "Convert the grey background to a solid pure white background (#FFFFFF). "
    "Keep the product itself unchanged — same shape, colour, lighting, and position. "
    "Do not crop or resize the product. Ensure background is evenly white and clean."
)

# Retry knobs (conservative; sequential processing)
RETRY_MAX_ATTEMPTS = 5
RETRY_BASE_DELAY_S = 1.5
RETRY_MAX_DELAY_S = 12.0

# -------------------- LOGGING --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("whitebg")

# -------------------- ENV + CLIENT --------------------
load_dotenv()

API_KEY = os.getenv("OPENAI_API_KEY")
if not API_KEY:
    sys.exit("❌ OPENAI_API_KEY not set in .env/environment")

# Safety: project keys should not use org headers
if os.getenv("OPENAI_ORG") or os.getenv("OPENAI_ORGANIZATION"):
    sys.exit("❌ Unset OPENAI_ORG / OPENAI_ORGANIZATION before running")

client = OpenAI(api_key=API_KEY)
logger.info("OpenAI client initialised")

# -------------------- NTFY --------------------
NTFY_TOPIC = os.getenv("NTFY_TOPIC")
NTFY_BASE = os.getenv("NTFY_BASE")
NTFY_URL = os.getenv("NTFY_URL")  # optional direct
NTFY_USERNAME = os.getenv("NTFY_USERNAME")  # optional
NTFY_PASSWORD = os.getenv("NTFY_PASSWORD")  # optional

# Default to the user-specified topic URL if env not provided
if not NTFY_URL:
    # You said: "the topic url is ntfy.sh/cooksdepo"
    # Respect env if given; else fall back to that.
    if NTFY_BASE and NTFY_TOPIC:
        NTFY_URL = f"{NTFY_BASE.rstrip('/')}/{NTFY_TOPIC.lstrip('/')}"
    else:
        NTFY_URL = "https://ntfy.sh/cooksdepo"


def _sanitize_http_header_value(v: str | None) -> str | None:
    if v is None:
        return None
    # Basic ASCII sanitiser for headers
    s = "".join(ch if 32 <= ord(ch) < 127 else "?" for ch in str(v))
    return s[:255]  # keep headers sane


def send_ntfy(title: str, message: str, priority: int = 5, tags: List[str] | None = None):
    if not NTFY_URL:
        logger.error("ntfy not configured; skipping notification")
        return
    safe_title = _sanitize_http_header_value(title)
    safe_priority = _sanitize_http_header_value(str(priority))
    safe_tags = ",".join(tags) if tags else None
    safe_tags = _sanitize_http_header_value(safe_tags) if safe_tags else None

    headers = {"Title": safe_title, "Priority": safe_priority}
    if safe_tags:
        headers["Tags"] = safe_tags

    auth = (NTFY_USERNAME, NTFY_PASSWORD) if (NTFY_USERNAME and NTFY_PASSWORD) else None

    try:
        r = requests.post(NTFY_URL, data=message.encode("utf-8"), headers=headers, auth=auth, timeout=15)
        if r.status_code // 100 != 2:
            logger.warning(f"ntfy responded {r.status_code}: {r.text[:500]}")
        else:
            logger.info(f"ntfy sent: {safe_title}")
    except Exception as e:
        logger.error(f"ntfy failed: {e}")


def notify_error(title: str, message: str, tags: List[str] | None = None):
    logger.error(f"{title} — {message}")
    send_ntfy(title=title, message=message, priority=5, tags=tags or ["rotating_light"])


def notify_warning(title: str, message: str, tags: List[str] | None = None):
    logger.warning(f"{title} — {message}")
    send_ntfy(title=title, message=message, priority=4, tags=tags or ["warning"])


# -------------------- RETRY WRAPPER --------------------
def retry_call(func, *args, **kwargs):
    attempts = RETRY_MAX_ATTEMPTS
    delay = RETRY_BASE_DELAY_S
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            return func(*args, **kwargs)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            last_exc = e
            if attempt == attempts:
                break
            jitter = random.uniform(0.7, 1.3)
            sleep_for = min(delay * jitter, RETRY_MAX_DELAY_S)
            logger.warning(f"{getattr(func,'__name__',str(func))} failed (attempt {attempt}/{attempts}): "
                           f"{e.__class__.__name__}: {e}. Retrying in {sleep_for:.1f}s …")
            time.sleep(sleep_for)
            delay = min(delay * 2, RETRY_MAX_DELAY_S)
    raise last_exc


# -------------------- OPENAI CALLS --------------------
def edit_images_batch(file_paths: List[Path]) -> List[bytes]:
    """
    Single /images/edits call for up to 4 images in one subfolder.
    Returns a list of raw PNG bytes (same order as file_paths).
    """
    # The Images API returns base64 data in response.data[i].b64_json
    def _call():
        with ExitStack() as stack:
            # open files safely and ensure closing
            files = [stack.enter_context(open(p, "rb")) for p in file_paths]
            resp = client.images.edit(
                model="gpt-image-1",
                image=files,                 # array of files
                prompt=PROMPT,
                background="opaque",
                output_format="png",
                quality="high",
                input_fidelity="high",
                size=FORCE_SIZE,
            )
            # Expect one output per input; if not, we'll handle below
            outs = []
            for item in resp.data:
                b64 = item.b64_json
                outs.append(base64.b64decode(b64))
            return outs

    return retry_call(_call)


def edit_image_single(file_path: Path) -> bytes:
    """
    Fallback per-image edit, used if batch fails or lengths mismatch.
    """
    def _call():
        with open(file_path, "rb") as f:
            resp = client.images.edit(
                model="gpt-image-1",
                image=f,
                prompt=PROMPT,
                background="opaque",
                output_format="png",
                quality="high",
                input_fidelity="high",
                size=FORCE_SIZE,
            )
        return base64.b64decode(resp.data[0].b64_json)

    return retry_call(_call)


# -------------------- MAIN WORKER --------------------
from contextlib import ExitStack  # after function defs to keep imports tidy


def process_subfolder(subdir: Path) -> Tuple[int, int]:
    """
    Process a single immediate subfolder.
    Returns (success_count, fail_count).
    """
    logger.info(f"Processing: {subdir}")
    imgs = sorted([p for p in subdir.iterdir() if p.is_file() and p.suffix.lower() in ACCEPTED_EXT])
    if not imgs:
        logger.info(f"No PNGs in {subdir.name}; skipping")
        return (0, 0)

    # Only take up to 4 as per your layout
    imgs = imgs[:BATCH_LIMIT]

    out_dir = subdir / OUTPUT_SUBDIR
    out_dir.mkdir(parents=True, exist_ok=True)

    success = 0
    fail = 0

    try:
        # Batch attempt
        outputs = edit_images_batch(imgs)

        if len(outputs) != len(imgs):
            # Unexpected; fallback per-image
            notify_warning("Batch/length mismatch",
                           f"{subdir.name}: expected {len(imgs)} outputs, got {len(outputs)}. Falling back per-image.")
            outputs = [None] * len(imgs)

        # Save those outputs we have; for missing, per-image retry below
        for i, src in enumerate(imgs):
            try:
                if outputs[i] is None:
                    raise ValueError("Missing output; will single-edit")
                data = outputs[i]
                dst = out_dir / (src.stem + ".png")
                # Overwrite (you asked to regenerate)
                with open(dst, "wb") as f:
                    f.write(data)
                success += 1
            except Exception as e:
                # Single fallback for that one
                try:
                    data = edit_image_single(src)
                    dst = out_dir / (src.stem + ".png")
                    with open(dst, "wb") as f:
                        f.write(data)
                    success += 1
                except Exception as e2:
                    fail += 1
                    notify_error("Image failed",
                                 f"{subdir.name}/{src.name}: {e2.__class__.__name__}: {e2}")

    except Exception as e:
        # Whole batch failed: try each file individually
        notify_warning("Batch failed; switching to per-image",
                       f"{subdir.name}: {e.__class__.__name__}: {e}")
        for src in imgs:
            try:
                data = edit_image_single(src)
                dst = out_dir / (src.stem + ".png")
                with open(dst, "wb") as f:
                    f.write(data)
                success += 1
            except Exception as e2:
                fail += 1
                notify_error("Image failed",
                             f"{subdir.name}/{src.name}: {e2.__class__.__name__}: {e2}")

    return (success, fail)


def main():
    if not ROOT.exists() or not ROOT.is_dir():
        sys.exit(f"❌ Root path does not exist or is not a directory: {ROOT}")

    total_ok = 0
    total_fail = 0
    scanned = 0

    # Only immediate subfolders
    for entry in sorted(ROOT.iterdir()):
        if entry.is_dir():
            scanned += 1
            ok, bad = process_subfolder(entry)
            total_ok += ok
            total_fail += bad

    summary = (f"Processed subfolders: {scanned}\n"
               f"Successful images:   {total_ok}\n"
               f"Failed images:       {total_fail}\n"
               f"Output location:     <subfolder>\\{OUTPUT_SUBDIR}")

    logger.info("\n" + summary)
    # Final summary notification (always helpful)
    send_ntfy(title="WhiteBG run complete",
              message=summary,
              priority=3,
              tags=["white_bg", "summary"])

    # Exit code useful for schedulers/CI
    if total_fail > 0:
        sys.exit(2)


if __name__ == "__main__":
    main()
