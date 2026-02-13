with raw_source as (
    select * from {{ source('raw_data', 'apps_metadata') }}
),

renamed as (
    select
        appId as app_id,
        title as app_name,
        description,
        summary,
        installs,
        minInstalls as min_installs,
        score as average_rating,
        ratings as ratings_count,
        reviews as reviews_count,
        histogram,
        price,
        free as is_free,
        (not free) as is_paid,
        currency,
        developer as developer_name,
        developerId as developer_id,
        developerEmail as developer_email,
        developerWebsite as developer_website,
        developerAddress as developer_address,
        privacyPolicy as privacy_policy,
        genreId as genre_id,
        genre as genre_name,
        icon,
        headerImage as header_image,
        screenshots,
        video,
        videoImage as video_image,
        contentRating as content_rating,
        contentRatingDescription as content_rating_description,
        adSupported as ad_supported,
        containsAds as contains_ads,
        released as released_date,
        updated as last_updated_timestamp,
        version as current_version,
        comments
    from raw_source
)

select * from renamed
where app_id is not null
