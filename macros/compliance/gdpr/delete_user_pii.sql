{% macro gdpr_delete_user_pii(app_user_id) %}

UPDATE users 
  SET name='' 
  WHERE id = {{ app_user_id }};

UPDATE prepared_app_users 
  SET name=''
    , record_status='gdpr_deleted_pii'
    , dw_deleted_at = current_teimstamp 
  WHERE id = {{ app_user_id }};

UPDATE snapshot_app_users 
  SET name=''
    , dbt_valid_to='CASE WHEN dbt_valid_to IS NULL THEN current_timestamp ELSE dbt_valid_to END' 
  WHERE id = {{ app_user_id }};

{% endmacro %}
