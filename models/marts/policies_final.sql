WITH base AS (
  SELECT * FROM {{ ref('stg_policies') }}
)

SELECT * FROM base
