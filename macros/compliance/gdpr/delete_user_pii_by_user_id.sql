{% macro gdpr_delete_user_pii(app_user_id) %}

{# UPDATE users 
    SET name='', updated_at = current_timestamp
    WHERE id = {{ app_user_id }}; #}

UPDATE prepared_app_users 
  SET name=''
    , record_status='gdpr_deleted_pii'
    , dw_deleted_at = current_timestamp 
  WHERE id = {{ app_user_id }};

-- mimic what dbt would do with making a hard delete into a soft delete, but delete the PII from all rows.
UPDATE snapshot_source_app_users 
  SET name=''
    , dbt_valid_to='CASE WHEN dbt_valid_to IS NULL THEN current_timestamp ELSE dbt_valid_to END' 
  WHERE id = {{ app_user_id }};

{% endmacro %}
