import json
import os
from datetime import datetime
from google_play_scraper import search, app, reviews_all

# --------------------------------------------------
# Configuration
# --------------------------------------------------
RAW_DATA_DIR = r"C:\Users\hp\Desktop\DATA-ENGINEERING\DATA-ENGINEERING\DATA-ENGINEERING-PROJECT\data\raw"
APPS_METADATA_PATH = os.path.join(RAW_DATA_DIR, "apps_metadata.json")
APPS_REVIEWS_PATH = os.path.join(RAW_DATA_DIR, "apps_reviews.jsonl")

SEARCH_QUERY = "AI note taking"
LANG = "en"
COUNTRY = "us"

# Ensure raw data directory exists
os.makedirs(RAW_DATA_DIR, exist_ok=True)


# --------------------------------------------------
# JSON serializer (for datetime objects)
# --------------------------------------------------
def json_serializer(obj):
    """
    Convert non-JSON-serializable objects into JSON-compatible formats.
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


# --------------------------------------------------
# Step 1: Discover apps
# --------------------------------------------------
def fetch_app_ids(query, lang="en", country="us", n_hits=20):
    """
    Search Google Play for AI note-taking apps and return their appIds.
    """
    results = search(
        query,
        lang=lang,
        country=country,
        n_hits=n_hits
    )
    return [app_data["appId"] for app_data in results]


# --------------------------------------------------
# Step 2: Fetch apps metadata
# --------------------------------------------------
def fetch_apps_metadata(app_ids):
    """
    Fetch metadata for each appId.
    """
    apps_data = []

    for app_id in app_ids:
        try:
            metadata = app(app_id, lang=LANG, country=COUNTRY)
            apps_data.append(metadata)
        except Exception as e:
            print(f"[WARN] Failed to fetch metadata for {app_id}: {e}")

    return apps_data


# --------------------------------------------------
# Step 3: Fetch apps reviews
# --------------------------------------------------
def fetch_apps_reviews(app_ids):
    """
    Fetch all reviews for each appId.
    """
    all_reviews = []

    for app_id in app_ids:
        try:
            reviews = reviews_all(
                app_id,
                lang=LANG,
                country=COUNTRY,
                sleep_milliseconds=0
            )

            for review in reviews:
                review["appId"] = app_id  # enrich with appId
                all_reviews.append(review)

        except Exception as e:
            print(f"[WARN] Failed to fetch reviews for {app_id}: {e}")

    return all_reviews


# --------------------------------------------------
# Step 4: Save raw data
# --------------------------------------------------
def save_json(data, path):
    """
    Save data as a JSON file.
    """
    with open(path, "w", encoding="utf-8") as f:
        json.dump(
            data,
            f,
            ensure_ascii=False,
            indent=2,
            default=json_serializer
        )


def save_jsonl(data, path):
    """
    Save data as a JSON Lines file.
    """
    with open(path, "w", encoding="utf-8") as f:
        for record in data:
            f.write(
                json.dumps(
                    record,
                    ensure_ascii=False,
                    default=json_serializer
                ) + "\n"
            )


# --------------------------------------------------
# Main pipeline
# --------------------------------------------------
def main():
    print("Searching for AI note-taking apps...")
    app_ids = fetch_app_ids(SEARCH_QUERY)

    print(f"Found {len(app_ids)} apps")

    print("Fetching apps metadata...")
    apps_metadata = fetch_apps_metadata(app_ids)
    save_json(apps_metadata, APPS_METADATA_PATH)

    print("Fetching apps reviews...")
    apps_reviews = fetch_apps_reviews(app_ids)
    save_jsonl(apps_reviews, APPS_REVIEWS_PATH)

    print("Raw data ingestion completed successfully.")
    print(f"- Apps metadata saved to: {APPS_METADATA_PATH}")
    print(f"- Apps reviews saved to: {APPS_REVIEWS_PATH}")


# --------------------------------------------------
# Entry point
# --------------------------------------------------
if __name__ == "__main__":
    main()
