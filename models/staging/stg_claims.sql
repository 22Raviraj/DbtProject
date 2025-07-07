{{ config(
  materialized='table',
  post_hook=["{{ log_issue('stg_claims', 'success', 'Staging completed') }}"]
) }}

SELECT * FROM {{ ref('raw_claims') }}  
 


