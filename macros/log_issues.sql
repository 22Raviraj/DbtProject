{% macro log_issue(model, status, message) %}
INSERT INTO {{ ref('audit_log') }} (
  run_invocation_id,
  model,
  status,
  message,
  updated_at
) VALUES (
  '{{ invocation_id }}',
  '{{ model }}',
  '{{ status }}',
  '{{ message }}',
  current_timestamp()
)
{% endmacro %}
