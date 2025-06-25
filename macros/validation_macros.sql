
{% macro validate_nulls(model_name, column_name) %}
SELECT '{{ model_name }}' AS model, '{{ column_name }}' AS column_name, COUNT(*) AS null_count
FROM {{ ref(model_name) }} WHERE {{ column_name }} IS NULL
{% endmacro %}

{% macro validate_mobile(model_name, column_name) %}
SELECT '{{ model_name }}' AS model, '{{ column_name }}' AS column_name, COUNT(*) AS invalid_mobile_count
FROM {{ ref(model_name) }} WHERE LENGTH({{ column_name }}) != 10 OR {{ column_name }} !~ '^[0-9]{10}$'
{% endmacro %}
