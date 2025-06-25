SELECT
  customer_id,
  first_name,
  last_name,
  email,
  phone
FROM {{ ref('customers') }}
