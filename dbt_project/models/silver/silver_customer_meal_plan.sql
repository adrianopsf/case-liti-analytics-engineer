WITH casted_data AS (
    SELECT 
        CAST(_id AS VARCHAR) AS customer_meal_plan_id,
        CAST("customerId" AS VARCHAR) AS customer_id,
        CAST("staffId" AS VARCHAR) AS staff_id,
        CAST("mealPlanId" AS VARCHAR) AS meal_plan_id,
        CAST(NULLIF("createdAt", '') AS TIMESTAMP) AS created_at,
        CAST(NULLIF("startDate", '') AS TIMESTAMP) AS start_date,
        CAST(NULLIF("endDate", '') AS TIMESTAMP) AS end_date,
        CAST(restrictions_vegan AS BOOLEAN) AS restrictions_vegan,
        CAST(restrictions_gluten AS BOOLEAN) AS restrictions_gluten,
        CAST(restrictions_lactose AS BOOLEAN) AS restrictions_lactose,
        CAST(restrictions_ovolacto AS BOOLEAN) AS restrictions_ovolacto,
        CAST("restrictions_highFodMaps" AS BOOLEAN) AS restrictions_high_fod_maps         
    FROM {{ source('bronze', 'bronze_customer_meal_plan') }}
),

base_data AS (
    SELECT 
        customer_meal_plan_id,
        customer_id,
        staff_id,
        meal_plan_id,
        created_at,
        start_date,
        end_date,

        /* Garantir os valores booleanos */
        COALESCE(restrictions_vegan, FALSE) AS restrictions_vegan,
        COALESCE(restrictions_gluten, FALSE) AS restrictions_gluten,
        COALESCE(restrictions_lactose, FALSE) AS restrictions_lactose,
        COALESCE(restrictions_ovolacto, FALSE) AS restrictions_ovolacto,
        COALESCE(restrictions_high_fod_maps, FALSE) AS restrictions_high_fod_maps

    FROM casted_data
    WHERE customer_meal_plan_id IS NOT NULL
      AND customer_id IS NOT NULL
      AND created_at <= CURRENT_DATE
      AND (start_date IS NULL OR start_date <= CURRENT_DATE)
      AND (end_date IS NULL OR end_date <= CURRENT_DATE)
)

-- Seleção dos dados finais
SELECT 
   *
FROM base_data

