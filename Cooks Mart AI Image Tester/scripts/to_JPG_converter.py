import os
import subprocess
import sys

# Full path to your ImageMagick executable
IMAGEMAGICK_PATH = r"C:\Program Files\ImageMagick-7.1.2-Q16-HDRI\magick.exe"

def convert_with_imagemagick(src_path, dest_path):
    """
    Use ImageMagick (the `magick.exe` at IMAGEMAGICK_PATH) to convert src → dest (as JPG).
    Returns True on success, False otherwise.
    """
    cmd = [
        IMAGEMAGICK_PATH,
        src_path,
        "-auto-orient",
        dest_path
    ]
    try:
        subprocess.run(cmd, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"ImageMagick failed converting {src_path} → {dest_path}: {e}")
        return False
    except FileNotFoundError as e:
        print(f"Could not find ImageMagick executable ({IMAGEMAGICK_PATH}): {e}")
        return False

def convert_to_jpg_and_delete(src_path):
    """
    If the file is an image we want to convert (heic, dng, etc.), convert it to JPG and delete orig.
    """
    root, ext = os.path.splitext(src_path)
    ext_low = ext.lower()
    dest = root + ".jpg"

    # Skip if it's already a jpg/jpeg
    if ext_low in [".jpg", ".jpeg"]:
        return

    # List of file extensions we attempt to convert
    convertible = {".heic", ".heif", ".dng", ".png", ".tif", ".tiff", ".bmp", ".gif"}
    if ext_low not in convertible:
        # skip unknown types
        return

    ok = convert_with_imagemagick(src_path, dest)
    if ok:
        try:
            os.remove(src_path)
        except Exception as e:
            print(f"Warning: could not delete original {src_path}: {e}")

def process_recursive(root_folder):
    """
    Walk through root_folder and its subfolders, converting files therein.
    """
    for dirpath, _, filenames in os.walk(root_folder):
        for fn in filenames:
            full = os.path.join(dirpath, fn)
            convert_to_jpg_and_delete(full)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python to_JPG_converter_fullpath.py <root_folder>")
        sys.exit(1)

    root = sys.argv[1]
    if not os.path.isdir(root):
        print(f"Error: {root} is not a valid directory.")
        sys.exit(1)

    process_recursive(root)
    print("Done.")