import os
import glob
import pandas as pd
import csv

def merge_csvs_quote_all(input_folder, output_file):
    csv_files = sorted(glob.glob(os.path.join(input_folder, "*.csv")))

    if not csv_files:
        raise ValueError("No CSV files found")

    print(f"Found {len(csv_files)} CSV files")

    dfs = []

    for file in csv_files:
        df = pd.read_csv(
            file,
            dtype=str,            # preserve everything
            na_filter=False       # no NaN guessing
        )
        dfs.append(df)
        print(f"Loaded: {os.path.basename(file)} → {df.shape}")

    merged_df = pd.concat(dfs, ignore_index=True)

    print(f"\nMerged shape: {merged_df.shape}")

    # 🔒 WRITE WITH FULL QUOTING
    merged_df.to_csv(
        output_file,
        index=False,
        sep=",",
        quoting=csv.QUOTE_ALL,
        escapechar="\\",
        encoding="utf-8"
    )

    print(f"\n✅ Final dataset written safely to:\n{output_file}")

    return merged_df


# ================== USAGE ==================
if __name__ == "__main__":
    input_folder = "/home/tejas/Desktop/phish_dataset/urls_and_dataset_chunks"
    output_file = "/home/tejas/Desktop/phish_dataset/New_NetPhish_Versions/NetPhish_2_V0.csv"

    merge_csvs_quote_all(input_folder, output_file)
