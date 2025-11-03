import os
import sys
import base64
from dotenv import load_dotenv
from openai import OpenAI

# Load your .env file
load_dotenv()

API_KEY = os.getenv("OPENAI_API_KEY")
if not API_KEY:
    sys.exit("❌ OPENAI_API_KEY not set in .env file or environment")

# Safety: ensure no organisation headers interfere
if os.getenv("OPENAI_ORG") or os.getenv("OPENAI_ORGANIZATION"):
    sys.exit("❌ Unset OPENAI_ORG / OPENAI_ORGANIZATION before running this test.")

client = OpenAI(api_key=API_KEY)

INPUT_IMAGE_PATH = "input.png"
OUTPUT_IMAGE_PATH = "output_white_bg.png"

try:
    print("🧠 Editing image...")
    response = client.images.edit(
        model="gpt-image-1",
        image=open(INPUT_IMAGE_PATH, "rb"),
        prompt=(
            "Convert the grey background to a solid pure white background (#FFFFFF). "
            "Keep the product itself unchanged — same shape, colour, lighting, and position. "
            "Do not crop or resize the product. "
            "Ensure background is evenly white and clean."
        ),
        background="opaque",       # ensures non-transparent background
        output_format="png",       # can also use "jpeg" or "webp"
        quality="high",            # supported for gpt-image-1
        input_fidelity="high",     # preserves original detail
        size="auto"                # let model decide proportions
    )

    # Extract base64 image data
    image_b64 = response.data[0].b64_json
    with open(OUTPUT_IMAGE_PATH, "wb") as f:
        f.write(base64.b64decode(image_b64))

    print(f"✅ Saved edited image as {OUTPUT_IMAGE_PATH}")

except Exception as e:
    print("❌ Failed to edit image:", e)
