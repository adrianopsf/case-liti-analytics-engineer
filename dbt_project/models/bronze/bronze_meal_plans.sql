WITH raw_meal_plans AS (
  SELECT * FROM {{ source('bronze', 'meal_plans') }}
)

SELECT * FROM raw_meal_plans