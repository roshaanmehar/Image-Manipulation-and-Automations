import os
import sys
import base64
import logging
import time
import random
from typing import Tuple, List
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import ExitStack

import requests
from dotenv import load_dotenv
from openai import OpenAI

# -------------------- CONFIG --------------------
ROOT = Path(r"C:\Roshaan\output_images\_MovedFiles")  # Root path where subfolders are located
CENTRAL_OUT = ROOT / "AIOPEN"                         # Central output folder (AIOPEN)
ACCEPTED_EXT = {".png"}                               # Only PNG files will be processed
FORCE_SIZE = "1024x1024"                              # Fixed output size for images

PROMPT = (
    "Convert the grey background to a solid pure white background (#FFFFFF). "
    "Keep the product itself unchanged — same shape, colour, lighting, and position. "
    "Do not crop or resize the product. Ensure background is evenly white and clean."
)

# Parallelism
DEFAULT_WORKERS = int(os.getenv("PARALLEL_WORKERS", "4"))  # Number of parallel threads (from .env)

# Retry knobs (exponential backoff for retries)
RETRY_MAX_ATTEMPTS = 5
RETRY_BASE_DELAY_S = 1.5
RETRY_MAX_DELAY_S = 12.0

# -------------------- LOGGING --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("whitebg_parallel")

# -------------------- ENV + BASIC VALIDATION --------------------
load_dotenv()

API_KEY = os.getenv("OPENAI_API_KEY")
if not API_KEY:
    sys.exit("❌ OPENAI_API_KEY not set in .env/environment")

# Project keys should not use org headers
if os.getenv("OPENAI_ORG") or os.getenv("OPENAI_ORGANIZATION"):
    sys.exit("❌ Unset OPENAI_ORG / OPENAI_ORGANIZATION before running")

# -------------------- NTFY --------------------
NTFY_TOPIC = os.getenv("NTFY_TOPIC")
NTFY_BASE = os.getenv("NTFY_BASE")
NTFY_URL = os.getenv("NTFY_URL")  # direct override
NTFY_USERNAME = os.getenv("NTFY_USERNAME")
NTFY_PASSWORD = os.getenv("NTFY_PASSWORD")

if not NTFY_URL:
    if NTFY_BASE and NTFY_TOPIC:
        NTFY_URL = f"{NTFY_BASE.rstrip('/')}/{NTFY_TOPIC.lstrip('/')}"
    else:
        NTFY_URL = "https://ntfy.sh/cooksdepo"  # Your fixed topic URL


def _sanitize_http_header_value(v: str | None) -> str | None:
    if v is None:
        return None
    s = "".join(ch if 32 <= ord(ch) < 127 else "?" for ch in str(v))
    return s[:255]


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
            sleep_for = min(delay * random.uniform(0.7, 1.3), RETRY_MAX_DELAY_S)
            logger.warning(
                f"{getattr(func,'__name__',str(func))} failed (attempt {attempt}/{attempts}): "
                f"{e.__class__.__name__}: {e}. Retrying in {sleep_for:.1f}s …"
            )
            time.sleep(sleep_for)
            delay = min(delay * 2, RETRY_MAX_DELAY_S)
    raise last_exc


# -------------------- OPENAI CALL --------------------
def edit_image_single(file_path: Path) -> bytes:
    """
    Single-image edit call to /v1/images/edits using gpt-image-1.
    Returns raw PNG bytes. Creates a fresh client per call for thread safety.
    """
    def _call():
        client = OpenAI(api_key=API_KEY)
        with open(file_path, "rb") as f:
            resp = client.images.edit(
                model="gpt-image-1",
                image=f,
                prompt=PROMPT,
                background="opaque",
                output_format="png",
                quality="high",
                input_fidelity="high",
                size="1024x1024",  # Ensure it's 1024x1024
            )
        return base64.b64decode(resp.data[0].b64_json)

    return retry_call(_call)


# -------------------- TASK PIPELINE --------------------
def discover_tasks() -> List[Tuple[Path, Path]]:
    """
    Find immediate subfolders and queue each PNG:
    (src_path, dst_path) where dst is under CENTRAL_OUT/<subfolder>/<filename>.png
    Skip images that already exist in the destination directory.
    """
    tasks: List[Tuple[Path, Path]] = []
    if not ROOT.exists() or not ROOT.is_dir():
        sys.exit(f"❌ Root path does not exist or is not a directory: {ROOT}")

    CENTRAL_OUT.mkdir(parents=True, exist_ok=True)

    for entry in sorted(ROOT.iterdir()):
        if entry.is_dir():
            sub_out = CENTRAL_OUT / entry.name
            sub_out.mkdir(parents=True, exist_ok=True)

            for p in sorted(entry.iterdir()):
                if p.is_file() and p.suffix.lower() in ACCEPTED_EXT:
                    dst = sub_out / (p.stem + ".png")
                    # Skip processing if the image already exists
                    if dst.exists():
                        logger.info(f"Skipping {p.name}, already exists in {sub_out}")
                        continue
                    tasks.append((p, dst))
    return tasks


def process_one(task: Tuple[Path, Path]) -> Tuple[bool, str]:
    """
    Process a single image task.
    Returns (ok, message) for summary accounting.
    """
    src, dst = task
    try:
        data = edit_image_single(src)
        with open(dst, "wb") as f:
            f.write(data)
        return True, f"OK {src.parent.name}/{src.name} -> {dst}"
    except Exception as e:
        notify_error("Image failed", f"{src.parent.name}/{src.name}: {e.__class__.__name__}: {e}")
        return False, f"FAIL {src.parent.name}/{src.name}: {e}"


def main():
    tasks = discover_tasks()
    if not tasks:
        logger.info("Nothing to do.")
        return

    logger.info(f"Discovered {len(tasks)} image(s). Starting pool with {DEFAULT_WORKERS} worker(s)...")

    successes = 0
    failures = 0

    # Sequential submission, parallel execution
    with ThreadPoolExecutor(max_workers=DEFAULT_WORKERS) as pool:
        future_map = {pool.submit(process_one, t): t for t in tasks}
        for fut in as_completed(future_map):
            ok, msg = fut.result()
            logger.info(msg)
            if ok:
                successes += 1
            else:
                failures += 1

    summary = (f"Parallel white-background run complete\n"
               f"Workers:            {DEFAULT_WORKERS}\n"
               f"Total images:       {len(tasks)}\n"
               f"Successful images:  {successes}\n"
               f"Failed images:      {failures}\n"
               f"Output root:        {CENTRAL_OUT}")

    logger.info("\n" + summary)
    send_ntfy(title="WhiteBG parallel run complete", message=summary, priority=3, tags=["white_bg", "summary"])

    if failures > 0:
        sys.exit(2)


if __name__ == "__main__":
    main()
