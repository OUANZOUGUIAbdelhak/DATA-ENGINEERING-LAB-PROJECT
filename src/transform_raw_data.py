import json
import os
from datetime import datetime
import pandas as pd

# --------------------------------------------------
# Configuration
# --------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed")

APPS_RAW_PATH = os.path.join(RAW_DIR, "apps_metadata.json")
REVIEWS_RAW_PATH = os.path.join(RAW_DIR, "apps_reviews.jsonl")

APPS_PROCESSED_PATH = os.path.join(PROCESSED_DIR, "apps_catalog.csv")
REVIEWS_PROCESSED_PATH = os.path.join(PROCESSED_DIR, "apps_reviews.csv")

os.makedirs(PROCESSED_DIR, exist_ok=True)

# --------------------------------------------------
# Helpers
# --------------------------------------------------

def parse_installs(value):
    if value is None:
        return None
    if isinstance(value, str):
        return int(value.replace(",", "").replace("+", ""))
    return value


def parse_price(value):
    if value in (None, "Free"):
        return 0.0
    if isinstance(value, str):
        return float(value.replace("$", ""))
    return float(value)


def parse_datetime(value):
    try:
        return pd.to_datetime(value)
    except Exception:
        return pd.NaT

# --------------------------------------------------
# Transform Apps Metadata
# --------------------------------------------------

def transform_apps_metadata():
    with open(APPS_RAW_PATH, "r", encoding="utf-8") as f:
        raw_apps = json.load(f)

    records = []
    for app in raw_apps:
        records.append({
            "appId": app.get("appId"),
            "title": app.get("title"),
            "developer": app.get("developer"),
            "score": app.get("score"),
            "ratings": app.get("ratings"),
            "installs": parse_installs(app.get("installs")),
            "genre": app.get("genre"),
            "price": parse_price(app.get("price"))
        })

    df = pd.DataFrame(records)
    df.drop_duplicates(subset=["appId"], inplace=True)

    df.to_csv(APPS_PROCESSED_PATH, index=False)
    return df

# --------------------------------------------------
# Transform Reviews
# --------------------------------------------------

def transform_reviews(apps_df):
    reviews = []

    with open(REVIEWS_RAW_PATH, "r", encoding="utf-8") as f:
        for line in f:
            reviews.append(json.loads(line))

    df = pd.DataFrame(reviews)

    df = df.rename(columns={
        "appId": "app_id",
        "content": "content",
        "score": "score",
        "reviewId": "reviewId",
        "userName": "userName",
        "thumbsUpCount": "thumbsUpCount",
        "at": "at"
    })

    df["at"] = df["at"].apply(parse_datetime)

    df = df.merge(
        apps_df[["appId", "title"]],
        left_on="app_id",
        right_on="appId",
        how="left"
    )

    df = df.rename(columns={"title": "app_name"})
    df.drop(columns=["appId"], inplace=True)

    df = df[[
        "app_id",
        "app_name",
        "reviewId",
        "userName",
        "score",
        "content",
        "thumbsUpCount",
        "at"
    ]]

    df.to_csv(REVIEWS_PROCESSED_PATH, index=False)
    return df

# --------------------------------------------------
# Main
# --------------------------------------------------

def main():
    apps_df = transform_apps_metadata()
    transform_reviews(apps_df)
    print("Transformation completed successfully.")


if __name__ == "__main__":
    main()
