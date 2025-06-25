SELECT
  policy_id,
  customer_id,
  start_date,
  end_date,
  premium
FROM {{ ref('policies') }}
