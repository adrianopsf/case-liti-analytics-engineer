WITH casted_data AS (
    SELECT 
        CAST(_id AS VARCHAR) AS objectives_id,
        CAST(key AS VARCHAR) AS key,
        CAST(tipo AS VARCHAR) AS tipo,
        CAST(entity AS VARCHAR) AS entity,
        CAST(display AS VARCHAR) AS display,
        CAST(objective AS VARCHAR) AS objective,
        CAST(description AS VARCHAR) AS description,
        CAST("displayUnit" AS VARCHAR) AS display_unit
  
    FROM {{ source('bronze', 'bronze_objectives') }}
),


base_data AS (
    SELECT 
        objectives_id,
        key,
        tipo,
        entity,
        display,
        objective,
        description,
        display_unit  
    FROM casted_data
    WHERE objectives_id IS NOT NULL
)

-- Seleção dos dados finais
SELECT 
   *
FROM base_data