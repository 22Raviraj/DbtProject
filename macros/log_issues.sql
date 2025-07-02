-- macros/log_issues.sql
-- depends_on: {{ ref('audit_log') }}

{% macro log_issue(model_name, severity_level, issue_message) %}
  {% set invocation = invocation_id %}
  {% set ts = modules.datetime.datetime.now().isoformat() %}

  {% set sql %}
    INSERT INTO MAGMUTUAL_INSURANCE.CORE.AUDIT_LOG (
      run_invocation_id,
      model,
      status,
      message,
      updated_at
    )
    VALUES (
      '{{ invocation }}',
      '{{ model_name }}',
      '{{ severity_level }}',
      '{{ issue_message | replace("'", "''") }}',
      current_timestamp()
    )
  {% endset %}

  {% do run_query(sql) %}
{% endmacro %}



{% macro log_model_event(event_name) %}
  {% set model_name = model.name if execute else 'unknown' %}
  {% do log_issue(model_name, 'info', event_name) %}
{% endmacro %}



{% macro log_run_end_results(results) %}
  {% for result in results %}
    {% set model_name = result.node.name %}
    {% set status = result.status %}
    {% set message = result.message if result.message else '' %}
    {% do log_issue(model_name, status, message) %}
  {% endfor %}
{% endmacro %}
