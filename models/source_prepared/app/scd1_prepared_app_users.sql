
WITH existing AS (SELECT user_id, dw_created_at, dw_updated_at FROM cleaned_app_users)​

SELECT 
  id as user_id
  , first_name
  , created_at as src_created_at
  , updated_at as src_updated_at
  , COALESCE(existing.dw_created_at, current_timestamp) as dw_created_at
  , CASE 
      WHEN source_users.updated_at > existing.dw_updated_at THEN current_timestamp 
      ELSE existing.dw_updated_at 
     END as dw_updated_at
FROM {{ source('src_app', 'scd1_users') }} as source_users
  LEFT JOIN existing on source_users.id = existing.user_id
