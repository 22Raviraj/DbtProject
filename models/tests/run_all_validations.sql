{{ log_model_event('validations_start') }}

SELECT * FROM (
    {{ assert_column_not_null('stg_customers', 'customer_id') }}
) AS not_null_check

UNION ALL

SELECT * FROM (
    {{ assert_column_unique('stg_customers', 'customer_id') }}
) AS unique_check

UNION ALL

SELECT * FROM (
    {{ expect_column_values_to_be_in_list('stg_customers', 'country', ["'US'", "'CA'", "'IN'"]) }}
) AS value_check

{{ log_model_event('validations_end') }}
