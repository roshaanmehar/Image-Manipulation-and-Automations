import os
import shutil

# Define source and destination directories
source_dir = r"C:\Roshaan\FINAL"
destination_dir = r"C:\Roshaan\front45"

# Ensure the destination folder exists
os.makedirs(destination_dir, exist_ok=True)

# Walk through all subfolders
for root, dirs, files in os.walk(source_dir):
    for file in files:
        # Check for exact match
        if file.endswith("_front_45_raw.png"):
            source_path = os.path.join(root, file)
            destination_path = os.path.join(destination_dir, file)
            
            try:
                # Copy file (not move)
                shutil.copy2(source_path, destination_path)
                print(f"Copied: {file}")
            except Exception as e:
                print(f"Error copying {file}: {e}")

print("All matching files have been copied successfully.")
