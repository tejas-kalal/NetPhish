import pandas as pd
import os

input_filepath = "/home/tejas/Desktop/phish_dataset/NetPhish_V1.csv"  
output_filepath = "/home/tejas/Desktop/phish_dataset/NetPhish_V2.csv"  

def process_csv_file(input_filepath, output_filepath, trusted_netblock_owners):
    print(f"Reading CSV file from: {input_filepath}")
    
    normalized_trusted_owners = [owner.lower().strip() for owner in trusted_netblock_owners]
    
    try:
        df = pd.read_csv(input_filepath, encoding='utf-8', on_bad_lines='skip')
    except TypeError:
        try:
            df = pd.read_csv(input_filepath, encoding='utf-8', error_bad_lines=False)
        except TypeError:
            df = pd.read_csv(input_filepath)
    
    print(f"Loaded data with {len(df)} rows")
    
    df['netblock_owner'] = df['netblock_owner'].fillna('')  
    df['netblock_owner_normalized'] = df['netblock_owner'].astype(str).str.lower().str.strip()
    
    df['is_netblock_trusted'] = df['netblock_owner_normalized'].apply(
        lambda x: 1 if x in normalized_trusted_owners else 0
    )

    df.drop('netblock_owner_normalized', axis=1, inplace=True)
    
    cols = list(df.columns)
    cols.remove('is_netblock_trusted')
    cols.insert(len(cols) - 1, 'is_netblock_trusted')
    df = df[cols]
    
    original_rows = len(df)
    df.drop_duplicates(inplace=True)
    print(f"Dropped {original_rows - len(df)} duplicate rows. Remaining rows: {len(df)}")
    
    df.to_csv(output_filepath, index=False)
    print(f"Processing complete. Output saved to: {output_filepath}")

trusted_netblock_owners = [
    "Google LLC",
    "Amazon.com, Inc.",
    "Amazon Data Services Japan",
    "Amazon Technologies Inc.",
    "Amazon Web Services, Elastic Compute Cloud, EC2",
    "Amazon Data Services Ireland Limited",
    "Amazon Data Services UK",
    "Amazon Data Services France",
    "Amazon Data Services Sweden",
    "Amazon Data Services Hong Kong",
    "Amazon Data Services Singapore",
    "Amazon Data Services Bahrain",
    "Amazon Data Services Brazil",
    "Amazon Data Services Italy",
    "Cloudflare, Inc.",
    "Microsoft Corporation",
    "Microsoft Limited UK",
    "Microsoft Corp",
    "Azure (implicitly through Microsoft)",
    "Google Cloud Asia Pacific Seoul Region",
    "Google Asia Pacific Pte. Ltd. (GAPPL)",
    "Google Fiber Inc.",
    "Akamai Technologies",
    "Akamai Technologies, Inc.",
    "IBM Cloud",
    "Oracle Corporation",
    "DigitalOcean, LLC",
    "DigitalOcean London",
    "Linode",
    "Scaleway",
    "Scaleway - Amsterdam, Netherlands",
    "Scaleway Dedibox - Paris, France",
    "OVH Hosting, Inc.",
    "OVH Ltd",
    "OVH Sp. z o. o.",
    "OVH SAS",
    "OVH BE",
    "OVH GmbH",
    "OVH Singapore PTE. LTD",
    "OVH US LLC",
    "Hetzner Online GmbH",
    "Hetzner Online AG",
    "Alibaba Cloud LLC",
    "Aliyun Computing Co., LTD",
    "Alibaba Cloud - US",
    "Alibaba Cloud - HK",
    "Alibaba Cloud (Singapore) Private Limited",
    "Tencent cloud computing (Beijing) Co., Ltd.",
    "Tencent Cloud Computing (Beijing) Co., Ltd",
    "Fastly, Inc.",
    "GitHub, Inc.",
    "GoDaddy.com, LLC",
    "CloudAccess.net, LLC",
    "Vultr Holdings, LLC",
    "Vultr Holdings LLC",
    "Canonical USA Inc.",
    "Canonical Group Limited",
    "Hostinger International Limited",
    "Hostinger Servers",
    "Hostinger US",
    "Cloudflare CDN network",
    "Namecheap, Inc.",
    "Rackspace Hosting",
    "Backblaze Inc",
    "Box.com",
    "Cloudscale.ch AG",
    "CloudLoop Pty Ltd",
    "Cloud Plus Pty Ltd",
    "Cloud Innovation Ltd",
    "Oracle Network Information Services",
    "Salesforce.com, Inc.",
    "Incapsula Inc",
    "Wix.Com, Inc.",
    "Squarespace, Inc.",
    "HubSpot, Inc.",
    "Facebook, Inc.",
    "DreamHost (as New Dream Network, LLC)",
    "IONOS Inc.",
    "IONOS SE",
    "WPX Hosting",
    "Shopify, Inc.",
    "Pantheon",
    "Bigcommerce Inc.",
    "Fly.io, Inc.",
    "E2E Networks Limited",
    "Azure (via Microsoft)",
    "Netlify (via Fastly or AWS)",
    "Cloudradium L.L.C"
]

if __name__ == "__main__":
    if not os.path.exists(input_filepath):
        print(f"Error: Input file {input_filepath} does not exist.")
    else:
        process_csv_file(input_filepath, output_filepath, trusted_netblock_owners)