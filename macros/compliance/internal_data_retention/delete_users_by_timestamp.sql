{% macro internal_data_retention_truncate_users(cutoff_timestamp) %}
{#DELETE FROM users WHERE created_at < {{ cutoff_timestamp }}; updated_at #}

DELETE FROM prepared_app_users WHERE src_created_at < {{cutoff_timestamp}}; 

UPDATE snapshot_app_users 
  SET dbt_valid_from = {{cutoff_timestamp}}
    , dbt_updated_at = {{cutoff_timestamp}} 
  WHERE dbt_valid_from < {{cutoff_timestamp}};

DELETE FROM snapshot_app_users WHERE dbt_valid_from < {{cutoff_timestamp}};

DELETE FROM snapshot_app_users WHERE updated_at < {{cutoff_timestamp}};

{% endmacro %}
