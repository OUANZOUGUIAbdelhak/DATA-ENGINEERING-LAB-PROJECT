{{
  config(
    materialized = 'incremental',
    unique_key = 'review_id'
  )
}}

with reviews as (
    select * from {{ ref('stg_playstore_reviews') }}
    {% if is_incremental() %}
      where cast(strftime(review_timestamp, '%Y%m%d') as integer) > (select coalesce(max(date_key), 0) from {{ this }})
    {% endif %}
),

apps as (
    select * from {{ ref('dim_apps_scd') }}
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
  and reviews.review_timestamp >= apps.dbt_valid_from
  and (apps.dbt_valid_to is null or reviews.review_timestamp < apps.dbt_valid_to)
where apps.app_key is not null
  and date_key is not null
