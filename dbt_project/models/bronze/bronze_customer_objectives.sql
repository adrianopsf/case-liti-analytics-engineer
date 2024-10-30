WITH raw_customer_objectives AS (
  SELECT * FROM {{ source('bronze', 'customer_objectives') }}
)

SELECT * FROM raw_customer_objectives