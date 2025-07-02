{% macro test_null_check(model, column_name) %}
    {% if execute %}
        {% set model_name = model.name %}
        {% set query %}
            select
                '{{ invocation_id }}' as run_invocation_id,
                '{{ model_name }}' as model,
                'error' as status,
                'Null check failed on column: {{ column_name }}' as message,
                current_timestamp() as updated_at
            from {{ model }}
            where trim({{ column_name }}) is null or length(trim({{ column_name }})) = 0
        {% endset %}

        {% set results = run_query(query) %}
        {% if results and results.rows and results.rows | length > 0 %}
            {% set log_query %}
                insert into MAGMUTUAL_INSURANCE.CORE.AUDIT_LOG (
                    run_invocation_id, model, status, message, updated_at
                )
                {{ query }}
            {% endset %}
            {% do run_query(log_query) %}
            
            -- Force failure if needed
            {% do run_query("select 1 / 0") %}
        {% endif %}
    {% endif %}
{% endmacro %}
