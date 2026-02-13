with distinct_developers as (
    select distinct
        developer_id,
        developer_name,
        developer_website,
        developer_email
    from {{ ref('stg_playstore_apps') }}
    where developer_id is not null
)

select
    row_number() over (order by developer_name) as developer_key,
    developer_name,
    developer_website,
    developer_email
from distinct_developers
