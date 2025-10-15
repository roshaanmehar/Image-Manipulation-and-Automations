#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gemini 2.5 SKU prompt generator (reference-aware, folder-per-image, testable).

- Reads product data from products.csv
- Walks subfolders of IMAGES_ROOT; each subfolder contains one image
- Extracts SKU code from folder name (e.g., "COOBA0002 - 6521083...")
- Sends that image + structured prompt to Gemini
- Appends to one master JSON and one master CSV immediately per SKU
"""

import os
import re
import json
import csv
import time
from pathlib import Path
from typing import Any, Dict, Optional
import pandas as pd

# ------------------------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------------------------

CSV_PATH = Path("products.csv")  # must contain "Code (CM)" and "Name / Description"
IMAGES_ROOT = Path(r"C:\Roshaan\iCloudSinglesConverted")      # your root with many subfolders
OUT_DIR = Path(r".")          # results live here
OUT_MASTER_JSON = OUT_DIR / "all_results.json"
OUT_MASTER_CSV  = OUT_DIR / "all_results.csv"

MODEL_NAME = "gemini-2.5-pro"     # or "gemini-2.5-flash"
API_KEY = os.getenv("GOOGLE_API_KEY", "PUT_YOUR_KEY_HERE")

MAX_RETRIES = 3
RETRY_DELAY = 2.0
TEST_MODE = False   # True => process one SKU then stop (for a quick sanity check)

# CSV columns
COL_CODE = "Code (CM)"
COL_NAME = "Name / Description"

# Allowed image extensions for the subfolder image
IMG_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff"}

# ------------------------------------------------------------------------------
# GEMINI CLIENT
# ------------------------------------------------------------------------------

try:
    from google import genai
    client = genai.Client(api_key=API_KEY)
except Exception as e:
    raise SystemExit("Install SDK with: pip install google-genai\n" + str(e))

# ------------------------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------------------------

def extract_code_from_folder(folder_name: str) -> Optional[str]:
    """
    Heuristic to pull a SKU-like token from folder name.
    Prefers patterns like LETTERS+DIGITS (e.g. COOBA0002).
    """
    name = folder_name.strip()
    first_chunk = re.split(r"[ \t\-_/]+", name)[0]
    m = re.search(r"[A-Za-z]{3,}\d{3,}", first_chunk)
    if m:
        return m.group(0).upper()
    tokens = re.findall(r"[A-Za-z0-9]+", name)
    if not tokens:
        return None
    tokens.sort(key=len, reverse=True)
    return tokens[0].upper()

def load_catalog(csv_path: Path) -> pd.DataFrame:
    # utf-8-sig handles BOMs (common with Excel exports)
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    df.columns = [c.strip() for c in df.columns]
    return df

def find_row_by_code(df: pd.DataFrame, code: str) -> Optional[pd.Series]:
    col = df[COL_CODE].astype(str).str.strip().str.upper()
    rows = df[col == code.upper()]
    if rows.empty:
        return None
    return rows.iloc[0]

def choose_folder_image(folder: Path) -> Optional[Path]:
    """Pick one image file from the folder (stable ordering)."""
    for p in sorted(folder.iterdir()):
        if p.is_file() and p.suffix.lower() in IMG_EXTS:
            return p
    return None

def build_prompt(product_name: str, product_code: str) -> str:
    """
    JSON-only instruction; each e-comm angle prompt is complete and independent,
    and all prompts mention using six reference images as grounding.
    """
    return f"""
You are an e-commerce product copywriter and AI image prompt engineer.
You will NOT generate images. You will generate a JSON object ONLY.

Each product has six REFERENCE IMAGES that the image generator (Nano Banana) will receive.
Assume those reference images are provided as visual grounding.

Return a JSON with:
- "product_code": "{product_code}"
- "product_name": "{product_name}"
- "description": one concise, factual paragraph (materials, finish, function).
- "ecomm_prompts": object with three fields: "top", "side", "front_45".
  Each field is a COMPLETE prompt that includes ALL of the following:
    • Mention the product code and product name
    • Explicitly instruct: "Use the six provided reference images as grounding to ensure accuracy."
    • Required camera angle (top-down orthographic / true side profile / front three-quarter at ~45°)
    • Background must be PURE WHITE (#FFFFFF), no grey or off-white, no gradients or textures
    • Square 1:1 framing with ~5–10% clean margin, product centred and fully visible
    • Professional studio lighting with soft, realistic shadows
    • Sharp focus, accurate colour and proportions faithful to the references
    • No props, hands, people, logos, watermarks, reflections, or text
    • Output preference: WebP if supported; otherwise exactly 2048×2048 (JPEG or PNG)
- "lifestyle_prompt": one COMPLETE prompt string that:
    • Mentions the product code and product name
    • Explicitly instructs: "Use the six provided reference images as grounding to ensure accuracy."
    • Places the product in a realistic everyday use context with tasteful composition
    • Is moderately specific (surface type, general lighting/mood) without overconstraining
    • Contains no branding, logos, or text overlays

OUTPUT JSON ONLY (no markdown, no commentary).

{{
  "product_code": "{product_code}",
  "product_name": "{product_name}",
  "description": "<paragraph>",
  "ecomm_prompts": {{
    "top": "<full top-view prompt>",
    "side": "<full side-view prompt>",
    "front_45": "<full 45° prompt>"
  }},
  "lifestyle_prompt": "<full lifestyle prompt>"
}}
"""

def ensure_outputs():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if not OUT_MASTER_JSON.exists():
        with open(OUT_MASTER_JSON, "w", encoding="utf-8") as jf:
            json.dump([], jf)
    if not OUT_MASTER_CSV.exists():
        with open(OUT_MASTER_CSV, "w", newline="", encoding="utf-8") as cf:
            writer = csv.writer(cf)
            writer.writerow([
                "Code (CM)", "Product Name", "Description",
                "Ecomm Top", "Ecomm Side", "Ecomm 45", "Lifestyle"
            ])

def append_results_row(data: Dict[str, Any]) -> None:
    """Append to master JSON and CSV immediately (crash-safe)."""
    # JSON
    with open(OUT_MASTER_JSON, "r+", encoding="utf-8") as jf:
        all_data = json.load(jf)
        all_data.append(data)
        jf.seek(0)
        json.dump(all_data, jf, ensure_ascii=False, indent=2)
        jf.truncate()
    # CSV
    with open(OUT_MASTER_CSV, "a", newline="", encoding="utf-8") as cf:
        w = csv.writer(cf)
        w.writerow([
            data.get("product_code", ""),
            data.get("product_name", ""),
            data.get("description", ""),
            data.get("ecomm_prompts", {}).get("top", ""),
            data.get("ecomm_prompts", {}).get("side", ""),
            data.get("ecomm_prompts", {}).get("front_45", ""),
            data.get("lifestyle_prompt", ""),
        ])

def call_gemini(prompt: str, image_path: Path, model: str) -> Dict[str, Any]:
    """
    Upload the representative image (extra grounding) and request JSON.
    IMPORTANT: With google-genai, pass files/text directly in a list to `contents`.
    """
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            img_file = client.files.upload(file=image_path)
            # >>> The key fix is here: contents is a list of File + str, not {"role":..., "parts":...}
            resp = client.models.generate_content(
                model=model,
                contents=[img_file, prompt],
                config={"response_mime_type": "application/json"},
            )
            text = getattr(resp, "text", None) or str(resp)
            data = json.loads(text)

            # Minimal schema check
            for k in ["product_code", "product_name", "description", "ecomm_prompts", "lifestyle_prompt"]:
                if k not in data:
                    raise ValueError(f"Missing key in JSON: {k}")
            for k in ["top", "side", "front_45"]:
                if k not in data["ecomm_prompts"]:
                    raise ValueError(f"Missing e-comm angle: {k}")
            return data

        except Exception as e:
            last_err = e
            if attempt < MAX_RETRIES:
                print(f"[WARN] {image_path.name}: {e} (retry {attempt})")
                time.sleep(RETRY_DELAY * attempt)
            else:
                raise

# ------------------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------------------

def main():
    if API_KEY == "PUT_YOUR_KEY_HERE":
        raise SystemExit("Set GOOGLE_API_KEY environment variable (or edit API_KEY).")

    ensure_outputs()
    df = load_catalog(CSV_PATH)

    processed = 0
    # Iterate only direct subfolders under IMAGES_ROOT
    for folder in sorted(p for p in IMAGES_ROOT.iterdir() if p.is_dir()):
        code = extract_code_from_folder(folder.name)
        if not code:
            print(f"[SKIP] Could not derive code from folder: {folder.name}")
            continue

        row = find_row_by_code(df, code)
        if row is None:
            print(f"[SKIP] Code not found in CSV: {code}  (folder: {folder.name})")
            continue

        product_name = str(row.get(COL_NAME, "")).strip()
        if not product_name:
            print(f"[SKIP] Name/Description empty for code: {code}")
            continue

        image_path = None
        for p in sorted(folder.iterdir()):
            if p.is_file() and p.suffix.lower() in IMG_EXTS:
                image_path = p
                break
        if not image_path:
            print(f"[SKIP] No image file found in: {folder}")
            continue

        print(f"[INFO] Processing {code} — {product_name} ({image_path.name})")
        prompt = build_prompt(product_name, code)

        try:
            data = call_gemini(prompt, image_path, MODEL_NAME)
        except Exception as e:
            print(f"[ERROR] Gemini failed for {code}: {e}")
            continue

        append_results_row(data)
        processed += 1
        print(f"[DONE] Wrote {code} to master JSON & CSV.")

        if TEST_MODE:
            print("[TEST MODE] Stopping after one SKU.")
            break

    print(f"Completed {processed} SKU(s). Results in: {OUT_DIR}")

# ------------------------------------------------------------------------------
# ENTRY
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    main()
