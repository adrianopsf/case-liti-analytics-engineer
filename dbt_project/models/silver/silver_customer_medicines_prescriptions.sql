WITH casted_data AS (
    SELECT 
        CAST(_id AS VARCHAR) AS customer_medicines_prescriptions_id,
        CAST("customerId" AS VARCHAR) AS customer_id,
        CAST("staffId" AS VARCHAR) AS staff_id,
        CAST("medicineId" AS VARCHAR) AS medicine_id,
        CAST(description AS VARCHAR) AS description,
        CAST(dosage AS VARCHAR) AS dosage,
        CAST(NULLIF("createdAt", '') AS TIMESTAMP) AS created_at,
        CAST(NULLIF("updatedAt", '') AS TIMESTAMP) AS updated_at        
    FROM {{ source('bronze', 'bronze_customer_medicines_prescriptions') }}
),

base_data AS (
    SELECT 
        customer_medicines_prescriptions_id,
        customer_id,
        staff_id,
        medicine_id,
        
        /* Tratando descrições */
        CASE 
           WHEN description NOT IN ('-', 'XXX', 'sad', 'Teste') THEN description
           ELSE NULL
        END AS description,

        dosage,
        created_at,
        updated_at
    FROM casted_data
    WHERE customer_medicines_prescriptions_id IS NOT NULL
      AND customer_id IS NOT NULL
      AND created_at <= CURRENT_DATE
      AND (updated_at IS NULL OR updated_at <= CURRENT_DATE)
)

-- Seleção dos dados finais
SELECT 
   *
FROM base_data
