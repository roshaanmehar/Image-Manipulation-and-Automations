import os
import re
import csv

def scan_for_csv(src_root, csv_path):
    # regex for matching pure digits OR capturing numeric part
    digits_only = re.compile(r"^\d+$")
    # to extract numeric part from a name: find longest contiguous digits substring
    digit_extract = re.compile(r"(\d+)")
    
    rows = []
    
    for dirpath, dirnames, _ in os.walk(src_root):
        for dirname in dirnames:
            full_path = os.path.join(dirpath, dirname)
            
            # list contents
            try:
                contents = os.listdir(full_path)
            except PermissionError:
                print(f"⚠ Permission denied: {full_path}")
                continue
            
            is_empty = (len(contents) == 0)
            name_has_non_digit = not digits_only.match(dirname)
            
            if is_empty or name_has_non_digit:
                # extract barcode: if dirname is purely digits, barcode = dirname
                # else try to extract first digits substring
                m = digits_only.match(dirname)
                if m:
                    barcode = m.group(0)
                else:
                    m2 = digit_extract.search(dirname)
                    if m2:
                        barcode = m2.group(1)
                    else:
                        barcode = ""  # no digits found
                
                # you can set reason if you like
                reason = ""
                if is_empty and name_has_non_digit:
                    reason = "empty & non_numeric"
                elif is_empty:
                    reason = "empty"
                elif name_has_non_digit:
                    reason = "non_numeric"
                
                rows.append({
                    "barcode": barcode,
                    "itemcode": "",
                    "reason": reason
                })
    
    # write CSV
    fieldnames = ["barcode", "itemcode", "reason"]
    with open(csv_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    
    print(f"Wrote {len(rows)} rows to CSV {csv_path}")

def main():
    # === Hardcode your paths here ===
    src_root = r"C:\Roshaan\Anisa Bhamji's files - matches_full_rows"
    csv_path = r"C:\Roshaan\Anisa Bhamji's files - matches_full_rows\folder_report.csv"
    # ================================
    
    scan_for_csv(src_root, csv_path)

if __name__ == "__main__":
    main()
