WITH raw_medicines AS (
  SELECT * FROM {{ source('bronze', 'medicines') }}
)

SELECT * FROM raw_medicines