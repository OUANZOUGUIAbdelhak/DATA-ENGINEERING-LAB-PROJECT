import json
import os
import pandas as pd

# --------------------------------------------------
# Configuration
# --------------------------------------------------
PROJECT_ROOT = os.getcwd() 
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed")

# Metadata Sources
APPS_RAW_PATH = os.path.join(RAW_DIR, "apps_metadata.json")
APPS_UPDATED_PATH = os.path.join(RAW_DIR, "note_taking_ai_apps_updated.csv")

# Review Sources
REVIEWS_JSONL_PATH = os.path.join(RAW_DIR, "apps_reviews.jsonl")
REVIEWS_BATCH2_PATH = os.path.join(RAW_DIR, "note_taking_ai_reviews_batch2.csv")
REVIEWS_DRIFT_PATH = os.path.join(RAW_DIR, "note_taking_ai_reviews_schema_drift.csv")
REVIEWS_DIRTY_PATH = os.path.join(RAW_DIR, "note_taking_ai_reviews_dirty.csv")

# Final Outputs
APPS_PROCESSED_PATH = os.path.join(PROCESSED_DIR, "apps_catalog.csv")
REVIEWS_PROCESSED_PATH = os.path.join(PROCESSED_DIR, "apps_reviews.csv")

os.makedirs(PROCESSED_DIR, exist_ok=True)

# --------------------------------------------------
# Helpers
# --------------------------------------------------
def parse_installs(value):
    if value is None: return None
    if isinstance(value, str):
        return int(value.replace(",", "").replace("+", ""))
    return value

def parse_price(value):
    if value in (None, "Free"): return 0.0
    if isinstance(value, str):
        return float(value.replace("$", ""))
    return float(value)


# --------------------------------------------------
# Transform Apps Metadata
# --------------------------------------------------
def transform_apps_metadata():
    """
    Ingests the updated CSV metadata, cleans numeric fields, 
    and enforces uniqueness on appId.
    """
    if not os.path.exists(APPS_UPDATED_PATH):
        print(f"[ERROR] Metadata not found at {APPS_UPDATED_PATH}")
        return pd.DataFrame()

    print(f"-> Processing Updated Metadata: {os.path.basename(APPS_UPDATED_PATH)}")
    
    # Read CSV, treating empty commas as NaN
    df = pd.read_csv(APPS_UPDATED_PATH, index_col=False, on_bad_lines='warn').dropna(how='all')

    # 1. Enforce Uniqueness (Handle duplicate appId) 
    initial_count = len(df)
    # We keep the first occurrence; logic could be changed to keep highest score
    df = df.drop_duplicates(subset=["appId"], keep="first")
    print(f"✓ Metadata Integrity: Removed {initial_count - len(df)} duplicate application IDs.")

    # 2. Clean Numeric Fields
    def clean_installs(val):
        if pd.isna(val): return 0
        return int(str(val).replace(",", "").replace("+", ""))

    df["installs"] = df["installs"].apply(clean_installs)
    df["score"] = pd.to_numeric(df["score"], errors='coerce').fillna(0.0)
    df["price"] = pd.to_numeric(df["price"], errors='coerce').fillna(0.0)

    df.to_csv(APPS_PROCESSED_PATH, index=False, sep=';')
    
    return df

# --------------------------------------------------
# Handling Schema Drift
# --------------------------------------------------
def normalize_csv_schema(df):
    """
    Detects column names and standardizes them to the internal schema.
    This handles Batch 2, Schema Drift, and future variations[cite: 154].
    """
    # Define mapping from possible raw names -> internal pipeline names
    mapping = {
        "appId": "app_id",
        "appTitle": "app_name",
        "review_id": "reviewId",
        "username": "userName",
        "rating": "score",
        "review_text": "content",
        "likes": "thumbsUpCount",
        "review_time": "at"
    }
    
    # Rename only the columns that actually exist in this specific dataframe
    rename_logic = {k: v for k, v in mapping.items() if k in df.columns}
    return df.rename(columns=rename_logic)

# --------------------------------------------------
# Transform Reviews
# --------------------------------------------------
def transform_reviews(apps_df):
    all_dfs = []
    # Add dirty dataset to sources
    raw_files = [REVIEWS_JSONL_PATH, REVIEWS_BATCH2_PATH, REVIEWS_DRIFT_PATH, REVIEWS_DIRTY_PATH]

    for file_path in raw_files:
        if not os.path.exists(file_path): continue
        print(f"-> Processing: {os.path.basename(file_path)}")
        
        if file_path.endswith(".jsonl"): # Depending on the exercice this needs to be deleted
            df_temp = pd.read_json(file_path, lines=True).rename(columns={"appId": "app_id"})
        else:
            # Handle string 'NULL' as actual NaN during read 
            df_temp = pd.read_csv(file_path, na_values=['NULL', 'nan']).dropna(how='all')
            df_temp = normalize_csv_schema(df_temp)

        if "app_name" not in df_temp.columns: df_temp["app_name"] = pd.NA
        all_dfs.append(df_temp)

    df = pd.concat(all_dfs, ignore_index=True)

    # 1. Handle Inconsistent Timestamps 
    df["at"] = pd.to_datetime(df["at"], format='mixed', errors='coerce')

    # 2. Handle Inconsistent Numeric Values 
    df["score"] = pd.to_numeric(df["score"], errors='coerce')
    df["thumbsUpCount"] = pd.to_numeric(df["thumbsUpCount"], errors='coerce').fillna(0)

    # 3. Filtering Strategy
    initial_count = len(df)
    df = df[
        (df["at"].notna()) & 
        (df["score"].between(1, 5))
    ]
    print(f"✓ Data Quality: Removed {initial_count - len(df)} invalid records (bad dates/ratings).")

    # 4. Join and Schema Enrichment
    df = df.drop_duplicates(subset=["reviewId"], keep="first")
    df = df.merge(apps_df[["appId", "title"]], left_on="app_id", right_on="appId", how="left")
    df["app_name"] = df["title"].fillna(df["app_name"])
    
    # Ensure score is integer for clean aggregation
    df["score"] = df["score"].astype(int)

    final_df = df[["app_id", "app_name", "reviewId", "userName", "score", "content", "thumbsUpCount", "at"]]
    final_df.to_csv(REVIEWS_PROCESSED_PATH, index=False, sep=';')
    print(f"✓ Total clean records saved: {len(final_df)}")
    return final_df

def main():
    apps_df = transform_apps_metadata()
    transform_reviews(apps_df)

if __name__ == "__main__":
    main()