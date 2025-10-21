import json

# Path to your JSON file
file_path = "1500_all_results.json"

# Load the JSON data
with open(file_path, "r", encoding="utf-8") as file:
    data = json.load(file)

# Extract all product codes
product_codes = [item["product_code"] for item in data]

# Count unique and duplicate codes
unique_codes = set(product_codes)
duplicates = [code for code in product_codes if product_codes.count(code) > 1]

print(f"Total products: {len(product_codes)}")
print(f"Unique product codes: {len(unique_codes)}")
print(f"Duplicate product codes found: {len(set(duplicates))}")

if duplicates:
    print("Duplicate codes:", set(duplicates))
