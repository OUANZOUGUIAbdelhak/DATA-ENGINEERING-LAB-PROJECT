with reviews as (
    select * from {{ ref('stg_playstore_reviews') }}
),

apps as (
    select * from {{ ref('dim_apps') }}
)

select
    reviews.review_id,
    apps.app_key,
    apps.developer_key,
    cast(strftime(reviews.review_timestamp, '%Y%m%d') as integer) as date_key,
    reviews.rating,
    reviews.thumbs_up_count,
    reviews.review_text,
    reviews.review_version
from reviews
inner join apps on reviews.app_id = apps.app_id
where apps.app_key is not null
  and date_key is not null
