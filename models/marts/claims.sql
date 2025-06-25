WITH base AS (
  SELECT * FROM {{ ref('stg_claims') }}
)

SELECT * FROM base
