{% set null_phones = validate_nulls('stg_customers', 'phone') %}
{% set invalid_phones = validate_mobile('stg_customers', 'phone') %}

{% if execute %}
  {% for row in dbt_utils.get_query_results_as_dict(null_phones) %}
    {% do log_issue('stg_customers', 'error', 'Null phone numbers found: ' ~ row['null_count']) %}
  {% endfor %}

  {% for row in dbt_utils.get_query_results_as_dict(invalid_phones) %}
    {% do log_issue('stg_customers', 'warning', 'Invalid phone numbers found: ' ~ row['invalid_mobile_count']) %}
  {% endfor %}
{% endif %}
