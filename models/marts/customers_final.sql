{{ config(
  materialized='table',
  post_hook=[
    "{{ test_null_check(this, 'first_name') }}",
    "{{ test_null_check(this, 'last_name') }}",
    "{{ test_null_check(this, 'phone_1') }}",
    "{{ record_count(this, ref('stg_customers'), '', '', {}) }}"
  ]
) }}

WITH base AS (
  SELECT * FROM {{ ref('stg_customers') }} 
)

SELECT * FROM base
