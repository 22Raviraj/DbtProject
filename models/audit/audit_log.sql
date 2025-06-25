CREATE TABLE IF NOT EXISTS {{ target.database }}.{{ target.schema }}.audit_log (
  run_invocation_id STRING,
  model STRING,
  status STRING,
  message STRING,
  updated_at TIMESTAMP
);
