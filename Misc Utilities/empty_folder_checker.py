import os
import re
import shutil

def scan_and_move(src_root, dest_root):
    """
    Scan subfolders under src_root. If a subfolder is empty, OR its name has any non-digit character,
    move that folder (with its contents) into dest_root (keeping folder name).
    """
    digits_only = re.compile(r"^\d+$")
    
    # Ensure dest_root exists
    os.makedirs(dest_root, exist_ok=True)
    
    # Walk from bottom up to avoid moving parent before children
    for dirpath, dirnames, _filenames in os.walk(src_root, topdown=False):
        for dirname in dirnames:
            full_src = os.path.join(dirpath, dirname)
            
            # Skip destination root if it’s inside the source tree accidentally
            # (just in case dest_root is within src_root)
            # We check that full_src is not dest_root or inside it
            try:
                if os.path.commonpath([full_src, dest_root]) == dest_root:
                    # full_src is inside dest_root — skip it
                    continue
            except ValueError:
                # On Windows, commonpath may error if drives differ; ignore
                pass
            
            # Check empty
            try:
                contents = os.listdir(full_src)
            except PermissionError:
                print(f"⚠ Permission denied (cannot list): {full_src}")
                continue
            
            is_empty = (len(contents) == 0)
            name_has_non_digit = not digits_only.match(dirname)
            
            if is_empty or name_has_non_digit:
                # Compute destination path
                dest_path = os.path.join(dest_root, dirname)
                
                # If dest_path already exists, we need to disambiguate
                if os.path.exists(dest_path):
                    # e.g., append a suffix or use a unique name
                    base = dirname
                    counter = 1
                    while True:
                        newname = f"{base}_{counter}"
                        new_dest = os.path.join(dest_root, newname)
                        if not os.path.exists(new_dest):
                            dest_path = new_dest
                            break
                        counter += 1
                
                try:
                    shutil.move(full_src, dest_path)
                    reason = []
                    if is_empty:
                        reason.append("empty")
                    if name_has_non_digit:
                        reason.append("name_has_non_digit")
                    reason_str = " & ".join(reason)
                    print(f"Moved `{full_src}` → `{dest_path}`  (reason: {reason_str})")
                except Exception as e:
                    print(f"Failed to move `{full_src}` → `{dest_path}`: {e}")

def main():
    # === Hardcode your paths here ===
    src_root = r"C:\Roshaan\Anisa Bhamji's files - matches_full_rows"
    dest_root = r"C:\Roshaan\Anisa Bhamji's files - matches_full_rows\quarantine_moved"
    # ================================
    
    scan_and_move(src_root, dest_root)

if __name__ == "__main__":
    main()
