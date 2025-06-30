
{{ config(
  materialized='table',
  post_hook=["{{ log_issue('stg_customers', 'success', 'Staging completed') }}"]
) }}

SELECT * FROM {{ ref('raw_customers') }}

