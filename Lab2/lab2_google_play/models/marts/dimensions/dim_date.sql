with date_range as (
    select
        min(cast(review_timestamp as date)) as min_date,
        max(cast(review_timestamp as date)) as max_date
    from {{ ref('stg_playstore_reviews') }}
),

dates as (
    select
        range as date_day
    from range(
        (select min_date from date_range),
        (select max_date + interval 1 day from date_range),
        interval 1 day
    )
)

select
    cast(strftime(date_day, '%Y%m%d') as integer) as date_key,
    date_day as date,
    year(date_day) as year,
    month(date_day) as month,
    quarter(date_day) as quarter,
    dayofweek(date_day) as day_of_week,
    case when dayofweek(date_day) in (0, 6) then true else false end as is_weekend -- 0 is Sunday, 6 is Saturday in some SQLs, DuckDB is 0-6? 
    -- DuckDB dayofweek: 0 is Sunday, 6 is Saturday.
    -- Wait, standard ISO is 1-7.
    -- DuckDB documentation: dayofweek(date) -> Returns the day of the week (Sunday = 0, Saturday = 6).
    -- So 0 and 6 are weekends.
from dates
