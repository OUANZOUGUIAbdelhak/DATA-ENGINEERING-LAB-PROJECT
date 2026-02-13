with raw_source as (
    select * from {{ source('raw_data', 'apps_reviews') }}
),

renamed as (
    select
        reviewId as review_id,
        userName as user_name,
        userImage as user_image,
        content as review_text,
        score as rating,
        thumbsUpCount as thumbs_up_count,
        reviewCreatedVersion as review_version,
        "at" as review_timestamp,
        replyContent as reply_content,
        repliedAt as replied_at_timestamp,
        appVersion as app_version,
        appId as app_id
    from raw_source
)

select 
    review_id,
    user_name,
    user_image,
    review_text,
    cast(rating as integer) as rating,
    cast(thumbs_up_count as integer) as thumbs_up_count,
    review_version,
    cast(review_timestamp as timestamp) as review_timestamp,
    reply_content,
    cast(replied_at_timestamp as timestamp) as replied_at_timestamp,
    app_version,
    app_id
from renamed
where review_id is not null
