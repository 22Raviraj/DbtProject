{{ config(materialized='table') }}

SELECT
    CAST(NULL AS STRING) AS run_invocation_id,
    CAST(NULL AS STRING) AS model,
    CAST(NULL AS STRING) AS status,
    CAST(NULL AS STRING) AS message,
    CURRENT_TIMESTAMP AS updated_at
WHERE FALSE
