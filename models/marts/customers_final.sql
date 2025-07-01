{{ config(
  materialized='table',
  post_hook=[reconcile_row_counts(this, ref('stg_customers'))]
) }}

WITH base AS (
  SELECT * FROM {{ ref('raw_customers') }}
)

SELECT * FROM base
