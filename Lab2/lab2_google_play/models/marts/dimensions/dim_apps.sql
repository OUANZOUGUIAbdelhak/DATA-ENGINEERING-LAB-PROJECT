with apps as (
    select * from {{ ref('stg_playstore_apps') }}
),

developers as (
    select * from {{ ref('dim_developers') }}
),

categories as (
    select * from {{ ref('dim_categories') }}
)

select
    row_number() over (order by apps.app_name) as app_key,
    apps.app_id,
    apps.app_name,
    developers.developer_key,
    categories.category_key,
    apps.price,
    apps.is_paid,
    apps.installs,
    apps.average_rating as catalog_rating,
    apps.ratings_count
from apps
left join developers on apps.developer_name = developers.developer_name -- Joining on name or ID depending on uniqueness. Using name as distinct_developers used distinct developer_id/name combo.
-- Ideally join on developer_id if propagated, but dim_developers used developer_id to distinct.
-- Let's check dim_developers logic: it selected developer_name. 
-- Wait, in dim_developers I should have kept developer_id to join back if I want to be safe.
-- But the requested schema for dim_developers does NOT have developer_id.
-- So I must join on developer_name.
left join categories on apps.genre_name = categories.category_name
