select * from (
  {{ assert_column_not_null('stg_customers', 'customer_id') }}
) as not_null_check

union all

select * from (
  {{ assert_column_unique('stg_customers', 'customer_id') }}
) as unique_check

union all

select * from (
  {{ expect_column_values_to_be_in_list('stg_customers', 'country', ["'USA'", "'Canada'", "'India'"]) }}
) as value_check
