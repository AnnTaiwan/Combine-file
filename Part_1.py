"""
Concatenate the KSXXXX and RFXXXX files.
The NCXXXX files are concatenated in Part 2 with the result csv file in Part 1.
"""

import pandas as pd
import os
import numpy as np

def handle_duplicate_columns(columns):
    """ Renames duplicate columns in the list by appending a suffix. """
    seen = {}
    for idx, column in enumerate(columns):
        if column in seen:
            seen[column] += 1
            columns[idx] = f"{column}_{seen[column]}"
        else:
            seen[column] = 0
    return columns

def read_and_process_file_v3(file_path, mandatory_columns):
    """ Reads a file, processes duplicate column names, and ensures mandatory columns contain values. """
    with open(file_path, 'r') as file:
        lines = file.readlines()

    # Finding the header line
    header_line_index = None
    for i, line in enumerate(lines):
        if "STNNBR" in line:  # Assuming 'STNNBR' is unique to the header line
            header_line_index = i
            break

    if header_line_index is None:
        raise ValueError(f"Header not found in file: {file_path}")

    # Extracting header and unit lines
    header_line = lines[header_line_index].strip().split(",")
    header_line = handle_duplicate_columns(header_line)
    unit_line = lines[header_line_index + 1].strip().split(",")

    # Reading the data from the line after the units, handling specific na_values
    data = pd.read_csv(file_path, skiprows=header_line_index + 2, names=header_line, 
                       na_values=[ 'N/A', ''])

    # Ensure mandatory columns have values and not marked as NaN
    for col in mandatory_columns:
        if col in data.columns:
            # Replace 0 with numpy.nan to indicate missing value
            data[col] = data[col].replace(0, np.nan)
            # Forward and backward fill to interpolate missing values
            data[col] = data[col].fillna(method='ffill').fillna(method='bfill')

    return data, unit_line

def align_and_deduplicate_dataframes(all_data_frames):
    """ Aligns columns of multiple dataframes, deduplicates them, and returns a single concatenated dataframe. """
    # Identifying all unique columns across the dataframes
    unique_columns = set()
    for df in all_data_frames:
        unique_columns.update(df.columns)

    # Creating an empty DataFrame with all unique columns
    aligned_df = pd.DataFrame(columns=list(unique_columns))

    # Concatenating all dataframes into the aligned dataframe
    for df in all_data_frames:
        aligned_df = pd.concat([aligned_df, df], axis=0, ignore_index=True, sort=False)

    # Reordering the columns to match the first dataframe's order
    first_df_columns = all_data_frames[0].columns
    aligned_df = aligned_df.reindex(columns=first_df_columns)

    return aligned_df

# Define the columns that must have values as per the user's instructions
mandatory_columns = ["CTDPRS", "CTDDEP", "CTDTMP", "CTDSAL", "THETA", "SIGTHT", "OXYGEN"]

# Path to the datafiles folder
data_folder_path = 'datafiles'

# Listing the files in the datafiles folder
data_files = os.listdir(data_folder_path)
data_files_paths = [os.path.join(data_folder_path, file) for file in data_files]

# Process and concatenate the files
all_data_v3 = []
unit_line_v3 = None
for file_path in data_files_paths:
    data, current_unit_line = read_and_process_file_v3(file_path, mandatory_columns)
    if unit_line_v3 is None:
        unit_line_v3 = current_unit_line
    all_data_v3.append(data)

# Aligning and deduplicating the dataframes
concatenated_aligned_df_v3 = align_and_deduplicate_dataframes(all_data_v3)

# Saving the final concatenated data, avoiding blank lines between rows
final_csv_path_v3 = 'concatenated_data_Part_1.csv'
with open(final_csv_path_v3, 'w', newline='') as file:  # Adding newline='' to avoid blank lines
    file.write(','.join(unit_line_v3) + '\n')  # Writing the unit line
    concatenated_aligned_df_v3.to_csv(file, index=False)  # Writing the concatenated data

print("CSV file created at:", final_csv_path_v3)
