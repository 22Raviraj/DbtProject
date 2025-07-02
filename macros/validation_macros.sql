{% macro assert_column_not_null(model_name, column_name) %}
  {% set validation_name = 'not_null_check' %}

  {% if execute %}
    {% set query %}
      SELECT '{{ model_name }}' AS model,
             '{{ column_name }}' AS "column",
             '{{ validation_name }}' AS check_type,
             NULL AS value,
             COUNT(*) AS failed_rows
      FROM {{ ref(model_name) }}
      WHERE {{ column_name }} IS NULL
    {% endset %}

    {% set results = run_query(query) %}
    {% set failed_rows = results.columns[4].values()[0] %}

    {% if failed_rows > 0 %}
      {% do log_issue(model_name, 'error', column_name ~ ' has ' ~ failed_rows ~ ' NULL values') %}
    {% endif %}
  {% endif %}

  SELECT '{{ model_name }}' AS model,
         '{{ column_name }}' AS "column",
         '{{ validation_name }}' AS check_type,
         NULL AS value,
         COUNT(*) AS failed_rows
  FROM {{ ref(model_name) }}
  WHERE {{ column_name }} IS NULL
{% endmacro %}


{% macro assert_column_unique(model_name, column_name) %}
  {% set validation_name = 'unique_check' %}

  {% if execute %}
    {% set query %}
      SELECT {{ column_name }} AS value,
             COUNT(*) AS dup_count
      FROM {{ ref(model_name) }}
      GROUP BY {{ column_name }}
      HAVING COUNT(*) > 1
    {% endset %}

    {% set results = run_query(query) %}
    {% for row in results.rows %}
      {% set val = row[0] %}
      {% set count = row[1] %}
      {% do log_issue(model_name, 'error', column_name ~ ' has duplicate value: ' ~ val ~ ' (' ~ count ~ ' times)') %}
    {% endfor %}
  {% endif %}

  SELECT '{{ model_name }}' AS model,
         '{{ column_name }}' AS "column",
         '{{ validation_name }}' AS check_type,
         {{ column_name }} AS value,
         COUNT(*) AS failed_rows
  FROM {{ ref(model_name) }}
  GROUP BY {{ column_name }}
  HAVING COUNT(*) > 1
{% endmacro %}


{% macro expect_column_values_to_be_in_list(model_name, column_name, allowed_values) %}
  {% set validation_name = 'value_check' %}
  {% set allowed_str = allowed_values | join(', ') %}

  {% if execute %}
    {% set query %}
      SELECT {{ column_name }} AS value,
             COUNT(*) AS cnt
      FROM {{ ref(model_name) }}
      WHERE {{ column_name }} NOT IN ({{ allowed_str }})
      GROUP BY {{ column_name }}
    {% endset %}

    {% set results = run_query(query) %}
    {% for row in results.rows %}
      {% set val = row[0] %}
      {% set count = row[1] %}
      {% do log_issue(model_name, 'error', column_name ~ ' has invalid value: ' ~ val ~ ' (' ~ count ~ ' times)') %}
    {% endfor %}
  {% endif %}

  SELECT '{{ model_name }}' AS model,
         '{{ column_name }}' AS "column",
         '{{ validation_name }}' AS check_type,
         {{ column_name }} AS value,
         COUNT(*) AS failed_rows
  FROM {{ ref(model_name) }}
  WHERE {{ column_name }} NOT IN ({{ allowed_str }})
  GROUP BY {{ column_name }}
{% endmacro %}


{% macro validate_foreign_key(child_model, child_column, parent_model, parent_column) %}
  {% set query %}
    SELECT c.*
    FROM {{ ref(child_model) }} c
    LEFT JOIN {{ ref(parent_model) }} p
      ON c.{{ child_column }} = p.{{ parent_column }}
    WHERE p.{{ parent_column }} IS NULL
  {% endset %}

  {% if execute %}
    {% set results = run_query(query) %}
    {% if results.columns and results.rows | length > 0 %}
      {% do log_issue(child_model, 'error', 'Foreign key ' ~ child_column ~ ' does not match ' ~ parent_model ~ '.' ~ parent_column) %}
    {% endif %}
  {% endif %}

  {{ return(query) }}
{% endmacro %}


{% macro run_custom_test(model, where_clause) %}
  {% set query %}
    SELECT *
    FROM {{ ref(model) }}
    WHERE {{ where_clause }}
  {% endset %}

  {% if execute %}
    {% set results = run_query(query) %}
    {% if results.columns and results.rows | length > 0 %}
      {% do log_issue(model, 'error', 'Custom condition failed: ' ~ where_clause) %}
    {% endif %}
  {% endif %}

  {{ return(query) }}
{% endmacro %}
