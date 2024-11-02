WITH weighin_base AS (
    SELECT
        owner_id as customer_id,
        created_at AS weighin_date,
        weight_kg,
        skeletal_muscle_kg,
        body_fat_percent,
        ROW_NUMBER() OVER (PARTITION BY owner_id ORDER BY created_at) AS weighin_number
    FROM {{ source('silver', 'silver_weighins') }}
    WHERE created_at IS NOT NULL
),


-- Calcula o Z-score para identificar os outliers em pesagens e peso muscular
zscore_calculations AS (
    SELECT
        *,
        (weight_kg - AVG(weight_kg) OVER (PARTITION BY customer_id)) / NULLIF(STDDEV(weight_kg) OVER (PARTITION BY customer_id), 0) AS weight_zscore,
        (skeletal_muscle_kg - AVG(skeletal_muscle_kg) OVER (PARTITION BY customer_id)) / NULLIF(STDDEV(skeletal_muscle_kg) OVER (PARTITION BY customer_id), 0) AS skeletal_muscle_zscore
    FROM weighin_base
),

-- Remove outliers
filtered_data AS (
    SELECT *
    FROM zscore_calculations
    WHERE ABS(weight_zscore) < 3 AND ABS(skeletal_muscle_zscore) < 3
),

weight_changes AS (
    SELECT
        customer_id,
        weighin_date,
        weight_kg,
        skeletal_muscle_kg,
        body_fat_percent,
        weighin_number,
        -- Calcula a mudança no peso total em relação ao peso anterior
        weight_kg - LAG(weight_kg) OVER (PARTITION BY customer_id ORDER BY weighin_date) AS weight_change,
        -- Calcula a mudança no músculo
        skeletal_muscle_kg - LAG(skeletal_muscle_kg) OVER (PARTITION BY customer_id ORDER BY weighin_date) AS skeletal_muscle_change,
        -- Calcula a porcentagem de mudança para peso e gordura corporal
        100 * (weight_kg - LAG(weight_kg) OVER (PARTITION BY customer_id ORDER BY weighin_date)) / NULLIF(LAG(weight_kg) OVER (PARTITION BY customer_id ORDER BY weighin_date), 0) AS weight_pct_change,
        100 * (body_fat_percent - LAG(body_fat_percent) OVER (PARTITION BY customer_id ORDER BY weighin_date)) / NULLIF(LAG(body_fat_percent) OVER (PARTITION BY customer_id ORDER BY weighin_date), 0) AS body_fat_pct_change
    FROM filtered_data
),



fact_weighins AS (
    SELECT
        w.customer_id,
        w.weighin_date,
        w.weight_kg,
        w.skeletal_muscle_kg,
        w.body_fat_percent,
        COALESCE(w.weight_change, 0) AS weight_change,
        COALESCE(w.skeletal_muscle_change, 0) AS skeletal_muscle_change,
        w.weight_pct_change,
        w.body_fat_pct_change
    FROM weight_changes w
)


SELECT * FROM fact_weighins
