WITH base_data AS (
    SELECT 
        *
    FROM {{ source('bronze', 'bronze_weighins') }}
    WHERE id IS NOT NULL
    AND age BETWEEN 1 AND 100
    AND height BETWEEN 50 AND 300
    AND (created_at IS NOT NULL OR created_at <= CURRENT_DATE) 
    AND (deleted_at IS NULL OR deleted_at <= CURRENT_DATE)
)

-- Seleção dos dados finais
SELECT 
   *
FROM base_data