WITH casted_data AS (
    SELECT 
        CAST(_id AS VARCHAR) AS customer_objectives_id,
        CAST("customerId" AS VARCHAR) AS customer_id,
        CAST("staffId" AS VARCHAR) AS staff_id,
        CAST(objective AS VARCHAR) AS objective,
        CAST(NULLIF("createdAt", '') AS TIMESTAMP) AS created_at,
        CAST(NULLIF("startDate", '') AS TIMESTAMP) AS start_date,
        CAST(NULLIF("updatedAt", '') AS TIMESTAMP) AS updated_at        
    FROM {{ source('bronze', 'bronze_customer_objectives') }}
),

base_data AS (
    SELECT 
        customer_objectives_id,
        customer_id,
        staff_id,
        objective,
    
        /* Assegurar que as datas não sejam futuras */
        CASE 
            WHEN start_date <= CURRENT_DATE THEN start_date
            ELSE NULL
        END AS start_date,

        CASE 
            WHEN updated_at <= CURRENT_DATE THEN updated_at
            ELSE NULL
        END AS updated_at,

        created_at  -- É verificado no WHERE
    FROM casted_data
    WHERE customer_objectives_id IS NOT NULL
      AND customer_id IS NOT NULL
      AND created_at <= CURRENT_DATE
)

-- Seleção dos dados finais
SELECT 
   *
FROM base_data

