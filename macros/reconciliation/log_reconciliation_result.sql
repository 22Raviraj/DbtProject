{% macro log_reconciliation_result(model_name, source_table, source_count, target_count) %}
  {% set status = "PASS" if source_count == target_count else "FAIL" %}
  {% set invocation = invocation_id() %}
  
  {% set sql %}
    INSERT INTO CORE.reconciliation_log (
      model_name, source_table, source_count, target_count, status, run_invocation_id, checked_at
    )
    VALUES (
      '{{ model_name }}',
      '{{ source_table }}',
      {{ source_count }},
      {{ target_count }},
      '{{ status }}',
      '{{ invocation }}',
      current_timestamp()
    )
  {% endset %}

  {% do run_query(sql) %}
{% endmacro %}
