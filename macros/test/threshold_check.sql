{% macro threshold_check(model, compare_model_name, column, operators, filter, additional_filter) %}
    {% set v_cfg_tbl_name = "adt_fpa_config" %}
    {% set v_fq_cfg_tbl_name = env_var('DBT_SF_SILVER_DB') ~ '.' ~ env_var('DBT_SF_SILVER_FPA_DATA') ~ '.' ~ v_cfg_tbl_name %}

    {% if execute %}

    -- Get threshold percentage
    {%- call statement("get_threshold_val", fetch_result=True) -%}
        select Threshold from {{ v_fq_cfg_tbl_name }}
        where Flag = 'Y' and Source = '{{ model.name }}'
    {%- endcall -%}
    {% set threshold_lst = load_result("get_threshold_val")["data"] %}
    {% if threshold_lst | length == 0 %}
        {{ exceptions.raise_compiler_error("Threshold missing from config table for " ~ model.name) }}
    {% endif %}
    {% set threshold_Per = threshold_lst[0][0] %}

    {% set union_all_sql %}

    with
    {%- for key, value in additional_filter.items() %}
        base_model_{{ key | replace(' ', '_') }} as (
            select count(*) as count_a, '{{ value }}' as source_name
            from {{ model }}
            where {{ column.source_column }} {{ operators.source_operators }} '{{ key }}'
        ),

        compare_model_{{ key | replace(' ', '_') }} as (
            select count(*) as count_b, '{{ value }}' as source_name
            from {{ compare_model_name }}
            where {{ column.target_column }} {{ operators.target_operators }} '{{ value }}'
        ),

        final_{{ key | replace(' ', '_') }} as (
            select
                (
                    case
                        when compare_model_{{ key | replace(' ', '_') }}.count_b = 0 then 0
                        else 100 * ((base_model_{{ key | replace(' ', '_') }}.count_a / compare_model_{{ key | replace(' ', '_') }}.count_b) - 1)
                    end
                )::number(38, 2) as threshold_per,
                '{{ model.name }}' as model,
                base_model_{{ key | replace(' ', '_') }}.source_name
            from base_model_{{ key | replace(' ', '_') }}, compare_model_{{ key | replace(' ', '_') }}
        )
        {{ "," if not loop.last }}
    {% endfor %}

    , union_all_result as (
        {% if additional_filter | length > 0 %}
            {% for key in additional_filter %}
                select * from final_{{ key | replace(' ', '_') }}
                where threshold_per <= -{{ threshold_Per }} or threshold_per >= {{ threshold_Per }}
                {% if not loop.last %} union all {% endif %}
            {% endfor %}
        {% else %}
            select null as threshold_per, '{{ model.name }}' as model, null as source_name where false
        {% endif %}
    )
    select * from union_all_result

    {% endset %}

    -- Create temporary table for failures
    {% set temp_table_name = "TEMP_UNION_RESULT_" ~ model.name | upper %}
    {% set create_temp_table %}
        create or replace temporary table {{ temp_table_name }} as
        {{ union_all_sql }}
    {% endset %}
    {% do run_query(create_temp_table) %}

    -- Insert into audit log
    {% set insert_log %}
        insert into MAGMUTUAL_INSURANCE.CORE.AUDIT_LOG (
            run_invocation_id, model, status, message, updated_at
        )
        select
            '{{ invocation_id }}',
            model,
            case when count(*) = 0 then 'pass' else 'fail' end,
            'Threshold check failed for some filters',
            current_timestamp()
        from {{ temp_table_name }}
    {% endset %}
    {% do run_query(insert_log) %}

    -- Force error if threshold check failed
    {% set force_error %}
        with status_cte as (
            select case when count(*) = 0 then 'pass' else 'fail' end as status
            from {{ temp_table_name }}
        )
        select 1 / iff(status = 'pass', 1, 0) as check_result
        from status_cte
    {% endset %}
    {% do run_query(force_error) %}

    {% endif %}
{% endmacro %}
