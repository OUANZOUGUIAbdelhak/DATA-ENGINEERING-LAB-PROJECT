import json
import os
import time
from datetime import datetime
from google_play_scraper import search, app, reviews, Sort

# --------------------------------------------------
# Configuration
# --------------------------------------------------
RAW_DATA_DIR = r"data\raw"
APPS_METADATA_PATH = os.path.join(RAW_DATA_DIR, "apps_metadata.json")
APPS_REVIEWS_PATH = os.path.join(RAW_DATA_DIR, "apps_reviews.jsonl")

SEARCH_QUERY = "AI note taking"
LANG = "en"
COUNTRY = "us"
MAX_PAGES_PER_APP = 10  # Pages to fetch
COUNT_PER_PAGE = 100   # Reviews per page

os.makedirs(RAW_DATA_DIR, exist_ok=True)

# --------------------------------------------------
# JSON serializer
# --------------------------------------------------
def json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

# --------------------------------------------------
# Step 1 & 2: Discover and Fetch Metadat
# --------------------------------------------------
def fetch_app_ids(query, n_hits=20):
    results = search(query, lang=LANG, country=COUNTRY, n_hits=n_hits)
    return [app_data["appId"] for app_data in results]

def fetch_apps_metadata(app_ids):
    apps_data = []
    for app_id in app_ids:
        try:
            metadata = app(app_id, lang=LANG, country=COUNTRY)
            apps_data.append(metadata)
        except Exception as e:
            print(f"[WARN] Failed metadata for {app_id}: {e}")
    return apps_data

# --------------------------------------------------
# Step 3: Fetch reviews with Pagination
# --------------------------------------------------
def fetch_apps_reviews_paginated(app_ids):
    """
    Fetch reviews using pagination to avoid rate limits.
    """
    all_reviews = []

    for app_id in app_ids:
        print(f"-> Fetching reviews for {app_id}...")
        continuation_token = None
        
        for page in range(MAX_PAGES_PER_APP):
            try:
                # Fetch a specific page of reviews
                result, continuation_token = reviews(
                    app_id,
                    lang=LANG,
                    country=COUNTRY,
                    sort=Sort.NEWEST,
                    count=COUNT_PER_PAGE,
                    continuation_token=continuation_token
                )
                
                for r in result:
                    r["appId"] = app_id
                    all_reviews.append(r)
                
                print(f"   Page {page+1}: Collected {len(result)} reviews.")
                
                # Stop if there are no more reviews
                if not continuation_token:
                    break
                
                # Production safety: Throttle requests to avoid bans
                time.sleep(1) 

            except Exception as e:
                print(f"[ERROR] Failed page {page+1} for {app_id}: {e}")
                break
                
    return all_reviews

# --------------------------------------------------
# Step 4: Save Data (Unchanged)
# --------------------------------------------------
def save_json(data, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=json_serializer)

def save_jsonl(data, path):
    with open(path, "w", encoding="utf-8") as f:
        for record in data:
            f.write(json.dumps(record, ensure_ascii=False, default=json_serializer) + "\n")

# --------------------------------------------------
# Main Pipeline
# --------------------------------------------------
def main():
    print("Searching for apps...")
    app_ids = fetch_app_ids(SEARCH_QUERY)
    
    print("Fetching metadata...")
    apps_metadata = fetch_apps_metadata(app_ids)
    save_json(apps_metadata, APPS_METADATA_PATH)

    print("Fetching reviews (Paginated)...")
    apps_reviews = fetch_apps_reviews_paginated(app_ids)
    save_jsonl(apps_reviews, APPS_REVIEWS_PATH)

    print(f"Completed. Total reviews collected: {len(apps_reviews)}")

if __name__ == "__main__":
    main()