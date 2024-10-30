WITH raw_customer_meal_plan AS (
  SELECT * FROM {{ source('bronze', 'customer_meal_plan') }}
)

SELECT * FROM raw_customer_meal_plan