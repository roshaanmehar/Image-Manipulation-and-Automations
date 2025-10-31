import os
import io
import base64
import requests
from openai import OpenAI
from PIL import Image

# --- SCRIPT CONFIGURATION ---

# 1. Set the root folder where your subfolders of images are located.
ROOT_FOLDER = r"C:\Roshaan\output_images\_MovedFiles"

# 2. Set the name for the main output folder.
OUTPUT_FOLDER_NAME = "OPENAI"

# 3. Set to True to only process the first 5 subfolders for a quick test.
TEST_MODE = True
TEST_MODE_SUBFOLDER_LIMIT = 5

# --- END OF CONFIGURATION ---


def initialize_openai_client():
    """
    Initializes and returns the OpenAI client.
    Reads the API key from the 'OPENAI_API_KEY' environment variable.
    """
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("ERROR: The 'OPENAI_API_KEY' environment variable is not set.")
        print("Please set the environment variable and try again.")
        return None
    return OpenAI(api_key=api_key)


def get_subfolders(path):
    """Returns a sorted list of subfolders in a given path."""
    try:
        return sorted([f.path for f in os.scandir(path) if f.is_dir()])
    except FileNotFoundError:
        return []


def get_image_files(path):
    """Returns a list of image files in a given path."""
    supported_formats = ('.png', '.jpg', '.jpeg', '.webp', '.bmp')
    return [file for file in os.listdir(path) if file.lower().endswith(supported_formats)]


def process_image_with_openai(image_path, client):
    """
    Uses GPT-4 Vision to analyze the product, then DALL-E 3 to generate
    a new version with a pure white background.
    
    Args:
        image_path (str): The full path to the source image.
        client (OpenAI): The initialized OpenAI client instance.

    Returns:
        str: The URL of the generated image, or None if an error occurred.
    """
    try:
        print(f"   - Analyzing with GPT-4 Vision: {os.path.basename(image_path)}")
        
        # Encode image to base64
        with open(image_path, "rb") as image_file:
            base64_image = base64.b64encode(image_file.read()).decode('utf-8')
        
        # Step 1: Use GPT-4 Vision to describe the product
        vision_response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": (
                                "Describe this product in detail for product photography recreation. "
                                "Focus on: the exact item type, its shape, size proportions, colors, "
                                "materials, textures, key features, and any text or branding visible. "
                                "Be specific and detailed but concise."
                            )
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/png;base64,{base64_image}"
                            }
                        }
                    ]
                }
            ],
            max_tokens=300
        )
        
        product_description = vision_response.choices[0].message.content
        print(f"   - Product description: {product_description[:100]}...")
        
        # Step 2: Use DALL-E 3 to generate the product with white background
        print(f"   - Generating with DALL-E 3...")
        dalle_prompt = (
            f"{product_description} "
            f"Professional product photography on a pure white background (#ffffff). "
            f"Clean, centered, well-lit, no shadows, no reflections, no text overlay."
        )
        
        dalle_response = client.images.generate(
            model="dall-e-3",
            prompt=dalle_prompt,
            size="1024x1024",
            quality="standard",
            n=1
        )
        
        image_url = dalle_response.data[0].url
        return image_url

    except Exception as e:
        print(f"   - ERROR during OpenAI API call for {os.path.basename(image_path)}: {e}")
        return None


def main():
    """Main function to orchestrate the image processing workflow."""
    print("--- Starting GPT-4 Vision + DALL-E 3 Background Correction Script ---")
    
    client = initialize_openai_client()
    if not client:
        return

    output_root_path = os.path.join(ROOT_FOLDER, OUTPUT_FOLDER_NAME)
    
    all_subfolders = get_subfolders(ROOT_FOLDER)
    if not all_subfolders:
        print(f"Error: No subfolders found in the directory: {ROOT_FOLDER}")
        return

    all_subfolders = [folder for folder in all_subfolders if os.path.basename(folder) != OUTPUT_FOLDER_NAME]

    folders_to_process = all_subfolders
    if TEST_MODE:
        print(f"\n--- TEST MODE ENABLED: Processing up to {TEST_MODE_SUBFOLDER_LIMIT} subfolders. ---\n")
        folders_to_process = all_subfolders[:TEST_MODE_SUBFOLDER_LIMIT]

    for folder_path in folders_to_process:
        folder_name = os.path.basename(folder_path)
        print(f"Processing folder: [{folder_name}]...")
        
        image_files = get_image_files(folder_path)
        if not image_files:
            print("   - No image files found in this folder. Skipping.")
            continue

        output_subfolder_path = os.path.join(output_root_path, folder_name)
        os.makedirs(output_subfolder_path, exist_ok=True)

        for image_name in image_files:
            original_image_path = os.path.join(folder_path, image_name)
            
            processed_image_url = process_image_with_openai(original_image_path, client)
            
            if processed_image_url:
                try:
                    response = requests.get(processed_image_url)
                    response.raise_for_status()

                    image_data = Image.open(io.BytesIO(response.content))
                    
                    base_name, _ = os.path.splitext(image_name)
                    output_image_path = os.path.join(output_subfolder_path, f"{base_name}.png")

                    image_data.save(output_image_path, "PNG")
                    print(f"   - ✔ Successfully saved to: {output_image_path}\n")

                except requests.exceptions.RequestException as e:
                    print(f"   - ERROR downloading image {image_name}: {e}\n")
                except IOError as e:
                    print(f"   - ERROR saving image {image_name}: {e}\n")

    print("--- Script finished. All folders processed. ---")


if __name__ == "__main__":
    main()