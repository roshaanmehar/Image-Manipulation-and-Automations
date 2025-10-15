import csv

def build_cm_to_barcode_map(input_csv_path, output_csv_path=None):
    """
    Reads the CSV at input_csv_path (with headers like “Code (CM)” and “Unit Barcode”),
    builds a dict mapping Code (CM) → Unit Barcode,
    and optionally writes the mapping to output_csv_path (as two columns).
    Returns the dict.
    """
    mapping = {}
    with open(input_csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        # Lower-strip header matching for flexibility
        headers = {h.lower().strip(): h for h in reader.fieldnames}
        # Required columns (case-insensitive)
        key_col = None
        val_col = None
        for h_lower, h in headers.items():
            if h_lower == "code (cm)":
                key_col = h
            elif h_lower == "unit barcode":
                val_col = h
        if key_col is None or val_col is None:
            raise ValueError(f"CSV must contain ‘Code (CM)’ and ‘Unit Barcode’ columns. Found: {reader.fieldnames}")
        
        for row in reader:
            cm = row.get(key_col, "").strip()
            ub = row.get(val_col, "").strip()
            if cm and ub:
                mapping[cm] = ub
    if output_csv_path:
        with open(output_csv_path, "w", newline="", encoding="utf-8") as fout:
            writer = csv.writer(fout)
            writer.writerow(["Code (CM)", "Unit Barcode"])
            for cm, ub in mapping.items():
                writer.writerow([cm, ub])
    return mapping


def main():
    input_csv = r"master_cooks_mart_products.csv"       # replace with your CSV path
    output_csv = r"itemcode_to_barcode.csv"    # replace if you want output
    mapping = build_cm_to_barcode_map(input_csv, output_csv_path=output_csv)
    print("Mappings (Code (CM) → Unit Barcode):")
    for cm, ub in mapping.items():
        print(f"{cm} → {ub}")


if __name__ == "__main__":
    main()
