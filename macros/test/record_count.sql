{% macro record_count(model, compare_model, source_custom_query, target_custom_query, where_condition={}, count_multiplier={}) %}
  {% set source_where = where_condition.base if (where_condition.base | length) else '' %}
  {% set target_where = where_condition.target if (where_condition.target | length) else '' %}
  {% set source_multiplier = count_multiplier.base if (count_multiplier.base | length) else 1 %}
  {% set target_multiplier = count_multiplier.target if (count_multiplier.target | length) else 1 %}

  {% if execute %}

    {% set source_where_builder = ("where " ~ source_where) if source_where | length else "" %}
    {% set target_where_builder = ("where " ~ target_where) if target_where | length else "" %}

    {% set validation_cte %}
      with
        base_model as (
          {% if target_custom_query | length %}
            select count as count_a from ( {{ target_custom_query }} )
          {% else %}
            select ({{ target_multiplier }} * count(*)) as count_a
            from {{ compare_model }} {{ target_where_builder }}
          {% endif %}
        ),

        compare_model as (
          {% if source_custom_query | length %}
            select count as count_b from ( {{ source_custom_query }} )
          {% else %}
            select ({{ source_multiplier }} * count(*)) as count_b
            from {{ model }} {{ source_where_builder }}
          {% endif %}
        ),

        combined as (
          select base_model.count_a, compare_model.count_b
          from base_model, compare_model
        )

      select
        count_a,
        count_b,
        case when count_a = count_b then 'pass' else 'fail' end as status,
        case
          when count_a != count_b then
            'Record count mismatch between {{ model }} and {{ compare_model }}: ' || count_b || ' vs ' || count_a
          else
            'Record count matched between {{ model }} and {{ compare_model }}: ' || count_b || ' vs ' || count_a
        end as message
      from combined
    {% endset %}

    -- ✅ Write to audit_log
    {% set audit_insert %}
      insert into MAGMUTUAL_INSURANCE.CORE.audit_log (
        run_invocation_id,
        model,
        status,
        message,
        updated_at
      )
      select
        '{{ invocation_id }}' as run_invocation_id,
        '{{ model }}' as model,
        status,
        message,
        current_timestamp() as updated_at
      from (
        {{ validation_cte }}
      ) as result
    {% endset %}
    {% do run_query(audit_insert) %}

    -- ❌ Fail build by triggering division-by-zero
    {% set fail_check %}
      with check_cte as (
        {{ validation_cte }}
      )
      select 1 / iff(status = 'pass', 1, 0) as force_error
      from check_cte
    {% endset %}
    {% do run_query(fail_check) %}

  {% endif %}
{% endmacro %}
