WITH casted_data AS (
    SELECT 
        CAST(_id AS VARCHAR) AS medicine_id,
        CAST(name AS VARCHAR) AS name,
        CAST(tipo AS VARCHAR) AS tipo,
        CAST(label AS VARCHAR) AS label,
        CAST(dosages_0_ AS VARCHAR) AS dosages_0_,
        CAST(dosages_1_ AS VARCHAR) AS dosages_1_,
        CAST(dosages_2_ AS VARCHAR) AS dosages_2_,
        CAST(dosages_3_ AS VARCHAR) AS dosages_3_,
        CAST(dosages_4_ AS VARCHAR) AS dosages_4_,
        CAST(dosages_5_ AS VARCHAR) AS dosages_5_,
        CAST(dosages_6_ AS VARCHAR) AS dosages_6_,
        CAST(dosages_7_ AS VARCHAR) AS dosages_7_,
        CAST(NULLIF("deletedAt", '') AS TIMESTAMP) AS deleted_at 
  
    FROM {{ source('bronze', 'bronze_medicines') }}
),


base_data AS (
    SELECT 
        medicine_id,
        name,
        tipo,
        label,
        /*Tratamento dos campos de dosagem */
        CASE 
            WHEN dosages_0_ = 'N/A' OR dosages_0_ = '' THEN NULL ELSE dosages_0_ 
        END AS dosage_0,
        CASE 
            WHEN dosages_1_ = 'N/A' OR dosages_1_ = '' THEN NULL ELSE dosages_1_ 
        END AS dosage_1,
        CASE 
            WHEN dosages_2_ = 'N/A' OR dosages_2_ = '' THEN NULL ELSE dosages_2_ 
        END AS dosage_2,
        CASE 
            WHEN dosages_3_ = 'N/A' OR dosages_3_ = '' THEN NULL ELSE dosages_3_ 
        END AS dosage_3,
        CASE 
            WHEN dosages_4_ = 'N/A' OR dosages_4_ = '' THEN NULL ELSE dosages_4_ 
        END AS dosage_4,
        CASE 
            WHEN dosages_5_ = 'N/A' OR dosages_5_ = '' THEN NULL ELSE dosages_5_ 
        END AS dosage_5,
        CASE 
            WHEN dosages_6_ = 'N/A' OR dosages_6_ = '' THEN NULL ELSE dosages_6_ 
        END AS dosage_6,
        CASE 
            WHEN dosages_7_ = 'N/A' OR dosages_7_ = '' THEN NULL ELSE dosages_7_ 
        END AS dosage_7,
       
        /* Assegurar que as datas não sejam datas futuras */
        CASE 
            WHEN deleted_at <= CURRENT_DATE THEN deleted_at 
            ELSE NULL
        END AS deleted_at
  
    FROM casted_data
    WHERE medicine_id IS NOT NULL
    AND name IS NOT NULL
)

-- Seleção dos dados finais
SELECT 
   *
FROM base_data


   