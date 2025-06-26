-- depends_on: {{ ref('audit_log') }}

{% if execute %}
  {% do log_issue('stg_customers', 'error', 'Null phone numbers found') %}
  {% do log_issue('stg_customers', 'warning', 'Invalid phone numbers found') %}
{% endif %}

WITH null_phones AS (
    SELECT
        'stg_customers' AS model,
        'phone_1' AS column_name,
        COUNT(*) AS issue_count,
        'error' AS severity,
        'Null phone numbers found' AS message
    FROM {{ ref('stg_customers') }}
    WHERE phone_1 IS NULL
),
invalid_phones AS (
    SELECT
        'stg_customers' AS model,
        'phone_1' AS column_name,
        COUNT(*) AS issue_count,
        'warning' AS severity,
        'Invalid phone numbers found' AS message
    FROM {{ ref('stg_customers') }}
    WHERE LENGTH(phone_1) != 10 OR NOT (phone_1 RLIKE '^[0-9]{10}$')
)

SELECT * FROM null_phones
UNION ALL
SELECT * FROM invalid_phones
