import os

def find_largest_files(base_folder):
    """
    Traverse all subfolders in the given folder and find the largest file in each.
    Prints the subfolder path, file name, and size (in MB).
    """
    for root, dirs, files in os.walk(base_folder):
        largest_size = 0
        largest_file = None

        for file in files:
            try:
                file_path = os.path.join(root, file)
                file_size = os.path.getsize(file_path)
                if file_size > largest_size:
                    largest_size = file_size
                    largest_file = file_path
            except (FileNotFoundError, PermissionError):
                continue  # Skip inaccessible files

        if largest_file:
            print(f"Folder: {root}")
            print(f"  Largest file: {os.path.basename(largest_file)}")
            print(f"  Size: {largest_size / (1024 * 1024):.2f} MB\n")

if __name__ == "__main__":
    folder_path = input("Enter the path to the folder: ").strip()
    if os.path.isdir(folder_path):
        find_largest_files(folder_path)
    else:
        print("Invalid folder path.")
