"""
Get the result file, called "final_concatenated_data.csv".
"""
import Part_1
import pandas as pd
import os
import numpy as np

def read_wat_file(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()
    
    # Find the header line
    header_line_index = None
    for i, line in enumerate(lines):
        if "STNNBR" in line:
            header_line_index = i
            break
    
    if header_line_index is not None:
        # Extracting the header line
        header_line = lines[header_line_index].strip().split(",")
        
        # Handling duplicate 'F' column names
        f_count = 0
        for i, col_name in enumerate(header_line):
            if col_name == 'F':
                if f_count > 0:
                    header_line[i] = f"F_{f_count}"
                f_count += 1

        # Read the data starting from the line after the unit line
        data = pd.read_csv(file_path, skiprows=header_line_index + 2, names=header_line, na_filter=False)
        return data
    else:
        raise ValueError("Header line with 'STNNBR' not found.")

# Continue with the rest of your code for concatenating the dataframes


# Define the directory where the .WAT files are located
wat_files_directory = 'NC_data'

# Read the existing CSV file
existing_csv_df = pd.read_csv('concatenated_data_Part_1.csv')


ori_header = existing_csv_df.columns.copy()
header = existing_csv_df.iloc[0].copy()

# Swap the first and second rows

# Set the new first row as column names
existing_csv_df.columns = header
existing_csv_df.iloc[0] = ori_header

header = pd.Series([i.strip() for i in existing_csv_df.columns])
existing_csv_df.columns = header

# Display the first few rows of the modified DataFrame
# existing_csv_df.head()
# print(existing_csv_df.columns)

# Get a list of all .WAT files in the directory
wat_files = [file for file in os.listdir(wat_files_directory) if file.endswith('.WAT')]

# Initialize an empty DataFrame to concatenate all .WAT files
all_wat_data = pd.DataFrame()

# Iterate over each .WAT file and concatenate the data
for wat_file in wat_files:
    file_path = os.path.join(wat_files_directory, wat_file)
    wat_df = read_wat_file(file_path)
    all_wat_data = pd.concat([all_wat_data, wat_df], ignore_index=True)
# Concatenate the .WAT data with the existing CSV data
# Aligning the columns and filling missing data if new columns were added from .WAT files

header = pd.Series([i.strip() for i in all_wat_data.columns])
all_wat_data.columns = header

final_df = pd.concat([existing_csv_df, all_wat_data], axis = 0, ignore_index=True)
final_df = final_df.reindex(columns=pd.unique(final_df.columns.tolist()))  # Remove duplicate columns
# final_df.fillna('N/A', inplace=True)  # Replace NaN with 'N/A'

# Save the concatenated DataFrame to a new CSV file
final_path = 'concatenated_data_complete.csv'
final_df.to_csv(final_path, index=False)
print("Final CSV file created at:", final_path)
