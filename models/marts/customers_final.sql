{{ config(
  materialized='table',
  post_hook=[
    "{{ record_count(this, ref('stg_customers'), '', '', {}) }}"
  ]
) }}

WITH base AS (
  SELECT * FROM {{ ref('raw_customers') }} 
)

SELECT * FROM base
