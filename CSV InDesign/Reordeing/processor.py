import os
import csv

# Set paths
root_directory = os.getcwd()  # Get the current working directory
print(f"Current working directory: {root_directory}")  # Debugging: Verify current directory

master_csv_path = os.path.join(root_directory, 'master.csv')  # Path to master CSV in the root directory
non_master_folder = root_directory  # Non-master CSV files are in the root directory as well
error_log_path = os.path.join(root_directory, 'error_log.txt')  # Path to error log file in the root directory

# Function to read the master CSV file and store data in a dictionary for quick lookup
def load_master_csv():
    master_data = {}
    with open(master_csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            master_data[row['Code (CM)']] = row
    return master_data

# Function to process each non-master CSV
def process_non_master_csv(non_master_file):
    # Load the master data for fast lookup
    master_data = load_master_csv()
    
    # Create the folder for this non-master file
    non_master_folder_name = os.path.splitext(non_master_file)[0]
    output_folder = os.path.join(root_directory, non_master_folder_name)  # Folder in the root directory
    os.makedirs(output_folder, exist_ok=True)
    
    # Prepare the output CSV path
    output_csv_path = os.path.join(output_folder, non_master_file)
    
    with open(output_csv_path, 'w', newline='', encoding='utf-8') as output_csv:
        writer = csv.writer(output_csv)
        
        # Write headers to the output CSV (taking headers from the master CSV)
        with open(master_csv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            writer.writerow(headers)
        
        # Open non-master file and process each code
        with open(os.path.join(non_master_folder, non_master_file), 'r', encoding='utf-8') as non_master:
            item_codes = non_master.readlines()
            
            # Initialize error log
            error_log = []
            
            for item_code in item_codes:
                item_code = item_code.strip()
                if item_code in master_data:
                    row = master_data[item_code]
                    writer.writerow(row.values())  # Write row data to the output file
                else:
                    # If item code is missing in the master CSV, write only the product code and leave other fields empty
                    empty_row = [item_code] + [''] * (len(headers) - 1)
                    writer.writerow(empty_row)
                    error_log.append(f"Item code {item_code} not found in master CSV.")
            
            # If there are errors, write them to the error log file
            if error_log:
                with open(error_log_path, 'a', encoding='utf-8') as error_file:
                    error_file.write(f"Errors for file: {non_master_file}\n")
                    for error in error_log:
                        error_file.write(f"{error}\n")
                    error_file.write("\n")

# Example: Process all non-master files in the folder
non_master_files = [f for f in os.listdir(non_master_folder) if f.endswith('.csv')]

for non_master_file in non_master_files:
    process_non_master_csv(non_master_file)

print("Processing complete.")
