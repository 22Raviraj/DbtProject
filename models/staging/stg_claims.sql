SELECT
  claim_id,
  policy_id,
  claim_date,
  amount,
  status
FROM {{ ref('claims') }}
