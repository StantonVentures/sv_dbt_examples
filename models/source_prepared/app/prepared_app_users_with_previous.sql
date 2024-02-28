
{%- set target_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name) -%}

WITH what_exists AS (
    SELECT user_id, dw_created_at, dw_updated_at 
    FROM {{ ref('seed_prepared_app_users_with_previous') }}
    {% if target_relation is not none %}
        UNION 
        SELECT user_id, dw_created_at, dw_updated_at 
        FROM {{this.database}}.{{this.schema}}.{{this.name}}
    {% endif %}
)

SELECT 
  id as user_id
  , first_name
  , created_at as src_created_at
  , updated_at as src_updated_at
  , COALESCE(what_exists.dw_created_at, current_timestamp) as dw_created_at
  , CASE 
      WHEN source_users.updated_at > what_exists.dw_updated_at THEN current_timestamp 
      ELSE what_exists.dw_updated_at 
    END as dw_updated_at
FROM {{ source('src_app', 'users') }} as source_users
  LEFT JOIN what_exists on source_users.id = what_exists.user_id
