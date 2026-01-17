# NetPhish Dataset Reproducibility Guide

This repository provides all scripts, notebooks, and intermediate files required to reproduce the **NetPhish phishing URL dataset**.  
Two reproducibility paths are provided:

1. **Easy & Quick Reproduction** – regenerate features directly from the final URL list  
2. **Full Source-Level Reproduction** – rebuild the dataset step-by-step from original data sources

---

## 1. Easy & Quick Reproducibility (Recommended for Most Users)

This method is intended for users who want to **quickly reproduce the feature set** without re-running the full data collection pipeline.

### Steps

1. Use the provided final dataset:
   - `NetPhish_2_V3.csv`

2. This file already contains:
   - URLs
   - Binary class labels (0 = legitimate, 1 = phishing)
   - A balanced class distribution

3. Extract the **URL column** from `NetPhish_2_V3.csv`.

4. Pass these URLs to:
   - `Netcraft_compliant_feature_extraction.ipynb`

5. The notebook will:
   - Render web pages using a standard browser configuration
   - Extract lexical and network-infrastructure features
   - Produce a newly generated feature CSV consistent with the original dataset

✅ This path allows **fast regeneration of features** while preserving dataset structure.

---

## 2. Full Dataset Reproducibility (From Original Sources)

This method reconstructs the dataset **from raw sources**, following the complete experimental protocol.

---

### Step 1: Legitimate URL Collection (Common Crawl)

1. Legitimate URLs were extracted from the **Common Crawl CC-MAIN-2025-13** crawl (mid-March 2025).

2. URLs were retrieved from the following WARC files:
   - `CC-MAIN-20250315031626-20250315061626-00000.warc.gz`
   - `CC-MAIN-20250315031626-20250315061626-00001.warc.gz`

3. These files can be downloaded from:
   - https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-13/index.html

4. URL extraction is performed using:
   - `extract_legitimate_url.ipynb`

5. This produces two CSV files:
   - `Legitimate_urls_A.csv`
   - `Legitimate_urls_B.csv`

6. Both files are merged to form:
   - `Legitimate_urls.csv`

---

### Step 2: Phishing URL Collection (PhishStats)

1. Phishing URLs were collected from **PhishStats**, which aggregates verified phishing reports.

2. Two snapshots were used:
   - April 9, 2025
   - May 2, 2025

3. The merged phishing URL file is provided as:
   - `phish_02may+09april.csv`

---

### Step 3: Feature Extraction

1. Both datasets:
   - `Legitimate_urls.csv`
   - `phish_02may+09april.csv`

   are passed to:

   - `Netcraft_compliant_feature_extraction.ipynb`

2. Feature extraction includes:
   - Lexical URL features
   - Network infrastructure features obtained via Netcraft Site Reports


### Step 4: URL–Feature Merging

1. URLs and their extracted features are merged using:
   - `url_dataset_merge.ipynb`

2. This produces:
   - One dataset for legitimate URLs
   - One dataset for phishing URLs

3. Both are combined using:
   - `legitimate_phishing_merge.py`

4. Output:
   - `NetPhish_2_V0.csv` (raw combined dataset)

---

### Step 5: Data Cleaning

1. `NetPhish_2_V0.csv` is processed using:
   - `Remove_Null.ipynb`

2. Operations performed:
   - Removal of rows with missing values 
   - Deduplication based on the URL column

3. Output:
   - `NetPhish_2_V1.csv`

---

### Step 6: Netblock Trust Feature Addition

1. `NetPhish_2_V1.csv` is passed to:
   - `is_netblock_trusted.py`

2. This adds:
   - `is_netblock_owner` feature

3. Output:
   - `NetPhish_2_V2.csv`

---

### Step 7: Final Preprocessing and Class Balancing

1. `NetPhish_2_V2.csv` is processed using:
   - `EDA_+_Preprocessing_NetPhish.ipynb`

2. Operations performed:
   - Dropping of `site_rank`, `domain_age`, and `netblock_owner`
   - Exploratory analysis and visualization
   - Class balancing via random undersampling

3. Class distribution before balancing:
   - Phishing (class 1): 33,570
   - Legitimate (class 0): 52,356

4. Legitimate class is undersampled to match phishing instances.

5. Final balanced dataset:
   - `NetPhish_2_V3.csv`

---

## Final Output

- **NetPhish_2_V3.csv**
  - Balanced phishing URL dataset
  - Ready for machine learning, benchmarking, and reproducibility studies

---

## Citation

If you use this dataset or pipeline, please cite the associated publication.

---

