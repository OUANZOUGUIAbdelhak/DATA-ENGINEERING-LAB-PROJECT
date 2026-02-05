import os
import pandas as pd

# --------------------------------------------------
# Configuration
# --------------------------------------------------
# Using os.getcwd() to ensure paths remain consistent with the transformation script
PROJECT_ROOT = os.getcwd()
PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed")

REVIEWS_PROCESSED_PATH = os.path.join(PROCESSED_DIR, "apps_reviews.csv")
APP_LEVEL_KPIS_PATH = os.path.join(PROCESSED_DIR, "app_level_kpis.csv")
DAILY_METRICS_PATH = os.path.join(PROCESSED_DIR, "daily_metrics.csv")

# Ensure processed directory exists
os.makedirs(PROCESSED_DIR, exist_ok=True)


# --------------------------------------------------
# Output 1: App-Level KPIs
# --------------------------------------------------
def generate_app_level_kpis(reviews_df):
    """
    Generate app-level KPIs from the unified reviews data.
    Metrics: num_reviews, avg_rating, pct_low_rating, review dates[cite: 105].
    """
    app_kpis = reviews_df.groupby(['app_id', 'app_name']).agg(
        num_reviews=('reviewId', 'count'),
        avg_rating=('score', 'mean'),
        low_rating_count=('score', lambda x: (x <= 2).sum()),
        first_review_date=('at', 'min'),
        most_recent_review_date=('at', 'max')
    ).reset_index()
    
    # Calculate percentage of low rating reviews (rating <= 2) [cite: 108]
    app_kpis['pct_low_rating'] = (
        app_kpis['low_rating_count'] / app_kpis['num_reviews'] * 100
    ).round(2)
    
    # Drop intermediate column and round average rating [cite: 107]
    app_kpis.drop(columns=['low_rating_count'], inplace=True)
    app_kpis['avg_rating'] = app_kpis['avg_rating'].round(2)
    
    return app_kpis


# --------------------------------------------------
# Output 2: Daily Metrics
# --------------------------------------------------
def generate_daily_metrics(reviews_df):
    """
    Generate daily time series metrics from the unified reviews data[cite: 112].
    """
    # Create a date-only column for aggregation
    temp_df = reviews_df.copy()
    temp_df['date'] = pd.to_datetime(temp_df['at']).dt.date
    
    # Aggregate by date [cite: 113, 114]
    daily_metrics = temp_df.groupby('date').agg(
        daily_num_reviews=('reviewId', 'count'),
        daily_avg_rating=('score', 'mean')
    ).reset_index()
    
    # Round metrics and sort for time-series consistency
    daily_metrics['daily_avg_rating'] = daily_metrics['daily_avg_rating'].round(2)
    daily_metrics.sort_values('date', inplace=True)
    
    return daily_metrics


# --------------------------------------------------
# Main Pipeline
# --------------------------------------------------
def main():
    """
    Main serving layer pipeline:
    Loads the merged processed data and generates analytics-ready outputs[cite: 20].
    """
    if not os.path.exists(REVIEWS_PROCESSED_PATH):
        print(f"[ERROR] Processed file not found: {REVIEWS_PROCESSED_PATH}")
        print("Please run transform_raw_data.py first.")
        return

    print(f"Loading merged processed data from: {REVIEWS_PROCESSED_PATH}")
    reviews_df = pd.read_csv(REVIEWS_PROCESSED_PATH, sep = ";")
    
    # Ensure 'at' is datetime (CSV stores as string) [cite: 95]
    reviews_df['at'] = pd.to_datetime(reviews_df['at'])
    
    print(f"✓ Loaded {len(reviews_df)} unique reviews (JSONL + CSV Batch).")
    
    # 1. Generate App-Level KPIs
    print("Generating app-level KPIs...")
    app_kpis = generate_app_level_kpis(reviews_df)
    app_kpis.to_csv(APP_LEVEL_KPIS_PATH, index=False)
    print(f"✓ Saved: {APP_LEVEL_KPIS_PATH} ({len(app_kpis)} apps)")
    
    # 2. Generate Daily Metrics
    print("Generating daily metrics...")
    daily_metrics = generate_daily_metrics(reviews_df)
    daily_metrics.to_csv(DAILY_METRICS_PATH, index=False)
    print(f"✓ Saved: {DAILY_METRICS_PATH} ({len(daily_metrics)} days)")
    


if __name__ == "__main__":
    main()