{% macro reconcile_row_counts(this_model_ref, source_model_ref=None) %}
  {% if execute %}
    {% set invocation_id_val = invocation_id %}
    {% set model_identifier = this_model_ref.identifier %}
    {% set model_schema = this_model_ref.schema %}
    {% set model_database = this_model_ref.database %}

    {# Dynamically guess the source model name if not passed #}
    {% if source_model_ref is none %}
      {% set source_table_name = 'stg_' ~ model_identifier %}
    {% else %}
      {% set source_table_name = source_model_ref.identifier %}
    {% endif %}

    {% set source_fq_name = model_database ~ '.' ~ model_schema ~ '.' ~ source_table_name %}

    {# Check if the source table exists #}
    {% set check_table_exists %}
      SELECT COUNT(*) AS exists_flag
      FROM {{ model_database }}.INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA = '{{ model_schema }}'
        AND TABLE_NAME = '{{ source_table_name.upper() }}'
    {% endset %}

    {% set exists_result = run_query(check_table_exists).columns[0].values()[0] %}

    {% if exists_result > 0 %}
      {# Build count queries #}
      {% set src_sql %}SELECT COUNT(*) AS cnt FROM {{ source_fq_name }}{% endset %}
      {% set tgt_sql %}SELECT COUNT(*) AS cnt FROM {{ this_model_ref }}{% endset %}

      {% set src_result = run_query(src_sql) %}
      {% set tgt_result = run_query(tgt_sql) %}

      {% if src_result and tgt_result %}
        {% set src_count = src_result.columns[0].values()[0] %}
        {% set tgt_count = tgt_result.columns[0].values()[0] %}

        {{ log("📊 Source count: " ~ src_count ~ " | Target count: " ~ tgt_count, info=True) }}

        {% if src_count is not none and tgt_count is not none %}
          {% set status = "pass" if src_count == tgt_count else "fail" %}

          {% set insert_sql %}
            INSERT INTO CORE.reconciliation_log (
              MODEL_NAME, SOURCE_TABLE, SOURCE_COUNT, TARGET_COUNT,
              STATUS, RUN_INVOCATION_ID, CHECKED_AT
            )
            VALUES (
              '{{ model_identifier }}',
              '{{ source_table_name }}',
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
      {% endif %}
    {% else %}
      {{ log("⚠️ Skipping reconciliation: Source table '" ~ source_fq_name ~ "' does not exist", info=True) }}
    {% endif %}
  {% endif %}
{% endmacro %}
