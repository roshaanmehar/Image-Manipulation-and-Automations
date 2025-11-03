import os
import io
import requests
from openai import OpenAI
from PIL import Image
from rembg import remove

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


def create_background_mask(image_path):
    """
    Creates a mask for DALL-E 2 editing.
    The mask should have transparent areas (alpha=0) where editing should occur (background),
    and opaque areas (alpha=255) where the image should remain unchanged (product).
    
    Args:
        image_path (str): Path to the original image.
    
    Returns:
        io.BytesIO: A byte stream containing the mask image in PNG format.
    """
    try:
        print(f"   - Creating background mask...")
        
        # Open the original image
        img = Image.open(image_path).convert("RGBA")
        
        # Use rembg to remove the background and get the product with transparency
        output = remove(img)
        
        # Extract the alpha channel from the rembg output
        # This alpha channel has: 255 (opaque) for product, 0 (transparent) for background
        alpha_channel = output.split()[3]
        
        # Create the mask image
        # DALL-E 2 mask format: transparent (alpha=0) = edit this area
        #                       opaque (alpha=255) = keep this area unchanged
        # Since we want to edit the background, we need:
        # - Background areas (currently alpha=0) should stay alpha=0 (transparent = edit)
        # - Product areas (currently alpha=255) should stay alpha=255 (opaque = keep)
        
        # Create an RGBA image where the alpha channel is our mask
        mask = Image.new('RGBA', img.size, (0, 0, 0, 0))
        
        # Apply the alpha channel: where product exists, make it opaque
        mask_data = []
        for pixel in alpha_channel.getdata():
            if pixel > 128:  # Product area
                mask_data.append((0, 0, 0, 255))  # Opaque - don't edit
            else:  # Background area
                mask_data.append((0, 0, 0, 0))  # Transparent - edit this
        
        mask.putdata(mask_data)
        
        # Convert to bytes
        mask_stream = io.BytesIO()
        mask.save(mask_stream, format='PNG')
        mask_stream.seek(0)
        
        return mask_stream
        
    except Exception as e:
        print(f"   - ERROR creating mask: {e}")
        return None


def process_image_with_openai(image_path, client):
    """
    Sends an image and mask to the OpenAI API (DALL-E 2) to replace its background.
    
    Args:
        image_path (str): The full path to the source image.
        client (OpenAI): The initialized OpenAI client instance.

    Returns:
        str: The URL of the generated image, or None if an error occurred.
    """
    try:
        print(f"   - Processing: {os.path.basename(image_path)}")
        
        # Open and prepare the image
        img = Image.open(image_path)
        rgba_img = img.convert("RGBA")
        
        # Save the image to a byte stream
        image_stream = io.BytesIO()
        rgba_img.save(image_stream, format='PNG')
        image_stream.seek(0)
        
        # Create the background mask
        mask_stream = create_background_mask(image_path)
        if not mask_stream:
            return None
        
        # Prepare the prompt
        prompt_text = (
            "Pure white background, #ffffff, solid white, "
            "no shadows, no gradients, clean white backdrop"
        )
        
        print(f"   - Sending to DALL-E 2 API...")
        
        # Call DALL-E 2 edit endpoint with both image and mask
        response = client.images.edit(
            model="dall-e-2",
            image=("image.png", image_stream),
            mask=("mask.png", mask_stream),
            prompt=prompt_text,
            n=1,
            size="1024x1024"
        )
        
        image_url = response.data[0].url
        return image_url

    except Exception as e:
        print(f"   - ERROR during OpenAI API call for {os.path.basename(image_path)}: {e}")
        return None


def main():
    """Main function to orchestrate the image processing workflow."""
    print("--- Starting DALL-E 2 with Mask Background Correction Script ---")
    print("--- Note: This requires 'rembg' package. Install with: pip install rembg ---\n")
    
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