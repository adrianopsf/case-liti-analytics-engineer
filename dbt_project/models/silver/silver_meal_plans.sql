WITH casted_data AS (
    SELECT 
        CAST(_id AS VARCHAR) AS meal_plan_id,
        CAST(name AS VARCHAR) AS name,
        CAST("group" AS VARCHAR) AS group_name,
        CAST(NULLIF("createdAt", '') AS TIMESTAMP) AS created_at     
    FROM {{ source('bronze', 'bronze_meal_plans') }}
),

base_data AS (
    SELECT 
        meal_plan_id,
        name,
        group_name,
        created_at 
    FROM casted_data
    WHERE meal_plan_id IS NOT NULL
      AND name IS NOT NULL
      AND created_at <= CURRENT_DATE
)

-- Seleção dos dados finais
SELECT 
   *
FROM base_data
