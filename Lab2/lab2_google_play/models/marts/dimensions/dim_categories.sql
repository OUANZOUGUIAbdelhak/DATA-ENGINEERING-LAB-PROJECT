with distinct_categories as (
    select distinct
        genre_id as category_id,
        genre_name as category_name
    from {{ ref('stg_playstore_apps') }}
    where genre_id is not null
)

select
    row_number() over (order by category_name) as category_key,
    category_name
from distinct_categories
