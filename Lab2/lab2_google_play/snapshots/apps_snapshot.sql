{% snapshot apps_snapshot %}

    {{
        config(
          target_schema='snapshots',
          unique_key='app_id',
          strategy='check',
          check_cols=['app_name', 'genre_name', 'price', 'is_paid', 'installs', 'average_rating', 'ratings_count']
        )
    }}

    select * from {{ ref('stg_playstore_apps') }}

{% endsnapshot %}
