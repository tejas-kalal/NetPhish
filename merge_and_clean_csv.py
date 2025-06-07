import os
import pandas as pd
import glob

def merge_and_clean_csvs(folder_path, output_file=None):
    csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
    
    if not csv_files:
        raise ValueError(f"No CSV files found in {folder_path}")
    
    print(f"Found {len(csv_files)} CSV files in {folder_path}")
    
    # Create an empty list to store DataFrames
    dfs = []
    
    # Read and append each CSV file to the list
    for file in csv_files:
        try:
            df = pd.read_csv(file)
            dfs.append(df)
            print(f"Successfully read {file} with {df.shape[0]} rows and {df.shape[1]} columns")
        except Exception as e:
            print(f"Error reading {file}: {e}")
    
    if not dfs:
        raise ValueError("No CSV files could be read successfully")
    
    # Concatenate all DataFrames into a single DataFrame
    merged_df = pd.concat(dfs, ignore_index=True)
    print(f"Merged DataFrame shape: {merged_df.shape}")
    
    # List of columns to check for null values
    columns_to_check = [
        "domain_age_days", "site_rank", "netblock_owner", "asn",
        "reverse_dns_present", "organisation", "dnsssec"
    ]
    
    # Count number of rows before cleaning
    rows_before = merged_df.shape[0]
    
    # Remove rows where ALL specified columns are null
    # (Keep rows where at least one of these columns has a non-null value)
    merged_df = merged_df[~merged_df[columns_to_check].isna().all(axis=1)]
    
    # Count number of rows after cleaning
    rows_after = merged_df.shape[0]
    rows_removed = rows_before - rows_after
    
    print(f"Number of rows before cleaning: {rows_before}")
    print(f"Number of rows after cleaning: {rows_after}")
    print(f"Number of rows removed: {rows_removed}")
    
    # Save the cleaned DataFrame to a CSV file if output_file is specified
    if output_file:
        merged_df.to_csv(output_file, index=False)
        print(f"Cleaned DataFrame saved to {output_file}")
    
    return merged_df

# Example usage:
if __name__ == "__main__":
    # Replace with your folder path
    folder_path = "/home/tejas/Desktop/phish_dataset/dataset_chunks"
    
    # Replace with your desired output file path (optional)
    output_file = "NetPhish_V1.csv"
    
    # Merge and clean the CSV files
    cleaned_df = merge_and_clean_csvs(folder_path, output_file)
    
    # Display sample of the cleaned DataFrame
    print("\nSample of the cleaned DataFrame:")
    print(cleaned_df.head())