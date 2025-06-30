{% macro reconcile_row_counts(this_model, source_model) %}
  {% set source_sql %}SELECT COUNT(*) FROM {{ ref(source_model) }}{% endset %}
  {% set target_sql %}SELECT COUNT(*) FROM {{ this_model }}{% endset %}

  {% set source_result = run_query(source_sql) %}
  {% set target_result = run_query(target_sql) %}

  {% if source_result and target_result %}
    {% set source_count = source_result.columns[0].values()[0] %}
    {% set target_count = target_result.columns[0].values()[0] %}
    {% do log_reconciliation_result(this_model.identifier, source_model, source_count, target_count) %}
  {% else %}
    {{ log("⚠️ Could not retrieve source/target counts for reconciliation.", info=True) }}
  {% endif %}
{% endmacro %}
