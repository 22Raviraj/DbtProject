{% macro reconcile_row_counts(this_model_ref, source_model_ref) %}
  {% set invocation_id_val = invocation_id %}

  {% set src_sql %}SELECT COUNT(*) AS cnt FROM {{ source_model_ref }}{% endset %}
  {% set tgt_sql %}SELECT COUNT(*) AS cnt FROM {{ this_model_ref }}{% endset %}

  {% set src_result = run_query(src_sql) %}
  {% set tgt_result = run_query(tgt_sql) %}

  {% if src_result and tgt_result %}
    {% set src_count = src_result.columns[0].values()[0] %}
    {% set tgt_count = tgt_result.columns[0].values()[0] %}

    {{ log("📊 Source count: " ~ src_count ~ " | Target count: " ~ tgt_count, info=True) }}

    {% if src_count is not none and tgt_count is not none %}
      {% set status = "pass" if src_count == tgt_count else "fail" %}
      {% set msg = "Source: " ~ src_count ~ ", Target: " ~ tgt_count %}

      {% set insert_sql %}
        INSERT INTO CORE.reconciliation_log (
          MODEL_NAME, SOURCE_TABLE, SOURCE_COUNT, TARGET_COUNT,
          STATUS, RUN_INVOCATION_ID, CHECKED_AT
        )
        VALUES (
          '{{ this_model_ref.identifier }}',
          '{{ source_model_ref.identifier }}',
          {{ src_count }},
          {{ tgt_count }},
          '{{ status }}',
          '{{ invocation_id_val }}',
          CURRENT_TIMESTAMP()
        )
      {% endset %}

      {{ log("📝 Inserting into reconciliation_log: " ~ insert_sql, info=True) }}
      {% do run_query(insert_sql) %}
    {% endif %}
  {% else %}
    {{ log("⚠️ Skipping reconciliation — one of the counts is missing", info=True) }}
  {% endif %}
{% endmacro %}
