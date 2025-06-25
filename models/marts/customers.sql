WITH base AS (
  SELECT * FROM {{ ref('stg_customers') }}
)

SELECT * FROM base
