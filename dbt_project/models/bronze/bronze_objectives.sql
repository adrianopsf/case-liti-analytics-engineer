WITH raw_objectives AS (
  SELECT * FROM {{ source('bronze', 'objectives') }}
)

SELECT * FROM raw_objectives