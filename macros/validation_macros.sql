-- macros/validations.sql

{% macro assert_column_not_null(model, column_name) %}
    {% set query %}
        SELECT
            'not_null_check' AS test_name,
            '{{ model }}' AS model,
            '{{ column_name }}' AS column,
            '{{ column_name }} has NULL values' AS message,
            COUNT(*) AS issue_count
        FROM {{ ref(model) }}
        WHERE {{ column_name }} IS NULL
    {% endset %}

    {% set results = run_query(query) %}
    {% if execute and results.columns and results.rows | length > 0 %}
        {% do log_issue(model, 'error', column_name ~ ' has null values') %}
    {% endif %}

    {{ return(query) }}
{% endmacro %}


{% macro assert_column_unique(model, column_name) %}
    {% set query %}
        SELECT
            'unique_check' AS test_name,
            '{{ model }}' AS model,
            '{{ column_name }}' AS column,
            '{{ column_name }} has duplicate values' AS message,
            COUNT(*) AS issue_count
        FROM {{ ref(model) }}
        GROUP BY {{ column_name }}
        HAVING COUNT(*) > 1
    {% endset %}

    {% set results = run_query(query) %}
    {% if execute and results.columns and results.rows | length > 0 %}
        {% do log_issue(model, 'error', column_name ~ ' has duplicate values') %}
    {% endif %}

    {{ return(query) }}
{% endmacro %}


{% macro expect_column_values_to_be_in_list(model, column_name, allowed_values) %}
    {% set query %}
        SELECT
            'value_check' AS test_name,
            '{{ model }}' AS model,
            '{{ column_name }}' AS column,
            '{{ column_name }} has invalid values' AS message,
            COUNT(*) AS issue_count
        FROM {{ ref(model) }}
        WHERE {{ column_name }} NOT IN ({{ allowed_values | join(', ') }})
    {% endset %}

    {% set results = run_query(query) %}
    {% if execute and results.columns and results.rows | length > 0 %}
        {% do log_issue(model, 'error', column_name ~ ' has values not in the allowed list') %}
    {% endif %}

    {{ return(query) }}
{% endmacro %}


{% macro validate_foreign_key(child_model, child_column, parent_model, parent_column) %}
    {% set query %}
        SELECT c.*
        FROM {{ ref(child_model) }} c
        LEFT JOIN {{ ref(parent_model) }} p
            ON c.{{ child_column }} = p.{{ parent_column }}
        WHERE p.{{ parent_column }} IS NULL
    {% endset %}

    {% set results = run_query(query) %}
    {% if execute and results.columns and results.rows | length > 0 %}
        {% do log_issue(child_model, 'error', 'Foreign key ' ~ child_column ~ ' does not match ' ~ parent_model ~ '.' ~ parent_column) %}
    {% endif %}

    {{ return(query) }}
{% endmacro %}

{% macro run_custom_test(model, where_clause) %}
    {% set query %}
        SELECT *
        FROM {{ ref(model) }}
        WHERE {{ where_clause }}
    {% endset %}

    {% set results = run_query(query) %}
    {% if execute and results.columns and results.rows | length > 0 %}
        {% do log_issue(model, 'error', 'Custom condition failed: ' ~ where_clause) %}
    {% endif %}

    {{ return(query) }}
{% endmacro %}
