{%- set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) -%}

WITH what_exists AS (
    -- Take what DW has existing as first part of UNION to assume our existing field data types. 
    -- This means we catch upstream field type changes, though it's limited to those that are auto-CAST incompatible
    -- This can auto-recover itself if upstream reverts to DW assumed data types(and they carry through EL). 
    -- Won't fail for field type changes that are auto-CAST compatible
    -- CAREFUL: If the upstream data type change is permanent and not auto-CAST compatible(or you forget to cast in `seeds` field definition), you have to [minimum] DROP the seed and recreate, then DROP this model and recreate 
        -- Exception: if change is auto-CAST compatible you can just add CASTs to the field(s) in the table-making query below
    {% if target_relation is not none %}
          -- list all fields so we know if upstream deletes a field. It's easier here than in a test because we don't have to flip between SQL and YAML files
        SELECT user_id::bigint, first_name::varchar(255), src_created_at::timestamptz, src_updated_at::timestamptz 
          , dw_created_at::timestamptz, dw_updated_at::timestamptz, dw_record_status::varchar(255), dw_deleted_at::timestamptz 
        FROM {{this.database}}.{{this.schema}}.{{this.name}}
        UNION
    {% endif %}
    SELECT * --* is on purpose so we get an error if we are missing a field in the seed
    FROM {{ ref('seed_prepared_app_users') }}
)

SELECT 
  COALESCE(source_users.id, what_exists.user_id) as user_id
  , COALESCE(source_users.first_name, what_exists.first_name) as first_name
  , COALESCE(source_users.created_at, what_exists.src_created_at) as src_created_at
  , COALESCE(source_users.updated_at, what_exists.src_updated_at) as src_updated_at

  , COALESCE(what_exists.dw_created_at, current_timestamp) as dw_created_at 
  , CASE 
      WHEN source_users.updated_at != what_exists.src_updated_at and what_exists.dw_updated_at is null THEN current_timestamp 
      ELSE what_exists.dw_updated_at 
    END as dw_updated_at
  , CASE
      WHEN what_exists.user_id is not null and source_users.id is null and what_exists.dw_record_status::varchar(255) is null THEN what_exists.dw_record_status::varchar(255) || ',deleted'
      WHEN what_exists.user_id is not null and source_users.id is null THEN what_exists.dw_record_status::varchar(255)
      ELSE what_exists.dw_record_status::varchar(255) 
    END as dw_record_status
  , CASE
      WHEN what_exists.user_id is not null and source_users.id is null and what_exists.dw_deleted_at::timestamptz is null THEN current_timestamp 
      ELSE what_exists.dw_deleted_at::timestamptz 
    END as dw_deleted_at
FROM {{ source('src_app', 'users') }} as source_users
  LEFT JOIN what_exists on source_users.id = what_exists.user_id


--CAREFUL: If source and DW timezone differ, then add a cast for all uses of source data/time(stamps)
