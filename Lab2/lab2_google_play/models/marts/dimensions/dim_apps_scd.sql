with apps as (
    select * from {{ ref('apps_snapshot') }}
),

developers as (
    select * from {{ ref('dim_developers') }}
),

categories as (
    select * from {{ ref('dim_categories') }}
)

select
    row_number() over (order by apps.app_name, apps.dbt_valid_from) as app_key,
    apps.app_id,
    apps.app_name,
    developers.developer_key,
    categories.category_key,
    apps.price,
    apps.is_paid,
    apps.installs,
    apps.average_rating as catalog_rating,
    apps.ratings_count,
    apps.dbt_valid_from,
    apps.dbt_valid_to,
    case when apps.dbt_valid_to is null then true else false end as is_current
from apps
left join developers on apps.developer_name = developers.developer_name
left join categories on apps.genre_name = categories.category_name
