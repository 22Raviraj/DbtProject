{{ config(materialized='ephemeral') }}

{% set sql %}
    SELECT X FROM {{ ref('test') }}  -- Invalid column
{% endset %}

{% do run_and_log_model('test1', sql) %}
