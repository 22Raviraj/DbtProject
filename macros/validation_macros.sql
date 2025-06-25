{% macro validate_nulls(model_name, column_name) %}
SELECT
  '{{ model_name }}' AS model,
  '{{ column_name }}' AS column,
  COUNT(*) AS null_count
FROM {{ ref(model_name) }}
WHERE {{ column_name }} IS NULL
{% endmacro %}

{% macro validate_mobile(model_name, column_name) %}
SELECT
  '{{ model_name }}' AS model,
  '{{ column_name }}' AS column,
  COUNT(*) AS invalid_mobile_count
FROM {{ ref(model_name) }}
WHERE LENGTH({{ column_name }}) != 10 OR {{ column_name }} !~ '^[0-9]{10}$'
{% endmacro %}

{% macro validate_row_count(model_name, min_count) %}
SELECT
  '{{ model_name }}' AS model,
  COUNT(*) < {{ min_count }} AS is_below_threshold,
  COUNT(*) AS actual_count
FROM {{ ref(model_name) }}
{% endmacro %}
