{% test record_count(model, compare_model, source_custom_query="", target_custom_query="", where_condition={}, count_multiplier={}) %}
    {{ test_record_count(
        model=model,
        compare_model=compare_model,
        source_custom_query=source_custom_query,
        target_custom_query=target_custom_query,
        where_condition=where_condition,
        count_multiplier=count_multiplier
    ) }}
{% endtest %}
