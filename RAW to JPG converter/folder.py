import os

def check_subfolders(base_folder):
    for root, dirs, files in os.walk(base_folder):
        # Skip the base folder itself
        if root == base_folder:
            continue

        # Filter out hidden files (optional)
        visible_files = [f for f in files if not f.startswith('.')]

        if len(visible_files) > 1:
            print(f"[INFO] Subfolder '{root}' contains more than one file ({len(visible_files)} files).")

        for file in visible_files:
            if not file.lower().endswith('.jpg'):
                print(f"[WARNING] File '{file}' in '{root}' is not a .jpg file.")

# Example usage
folder_path = r'C:\Roshaan\iCloudSinglesConverted'  # <-- Replace with your actual folder path
check_subfolders(folder_path)
