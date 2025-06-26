SELECT
  Customer_Id AS customer_id,
  First_Name AS first_name,
  Last_Name AS last_name,
  Country AS country,
  Phone_1 AS phone_1
FROM {{ ref('raw_customers') }}
