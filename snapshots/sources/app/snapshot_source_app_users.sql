{% snapshot snapshot_source_app_users %}
{{ config( unique_key="id"
    , strategy="timestamp", updated_at="updated_at"
    ) }}

SELECT *, '' as record_status, NULL as deleted_at
FROM {{ source('src_app', 'users') }}

{% endsnapshot %}