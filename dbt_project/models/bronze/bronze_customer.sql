WITH raw_customer AS (
  SELECT * FROM {{ source('bronze', 'customer') }}
)

SELECT * FROM raw_customer