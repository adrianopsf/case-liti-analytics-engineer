WITH weighin_base AS (
    SELECT
        owner_id AS customer_id,
        created_at AS weighin_date,
        weight_kg,
        skeletal_muscle_kg,
        body_fat_percent,
        ROW_NUMBER() OVER (PARTITION BY owner_id ORDER BY created_at) AS weighin_number
    FROM {{ source('silver', 'silver_weighins') }}
    WHERE created_at IS NOT NULL
),

-- Calcula estatísticas semanais para identificar comportamentos incomuns por semana
weekly_stats AS (
    SELECT
        customer_id,
        DATE_TRUNC('week', weighin_date) AS week_start_date,
        AVG(weight_kg) AS weekly_avg_weight,
        STDDEV(weight_kg) AS weekly_stddev_weight,
        COUNT(*) AS num_weighins
    FROM weighin_base
    GROUP BY customer_id, DATE_TRUNC('week', weighin_date)
),

weighins_with_weekly_stats AS (
    SELECT
        w.customer_id,
        w.weighin_date,
        w.weight_kg,
        w.skeletal_muscle_kg,
        w.body_fat_percent,
        w.weighin_number,
        ws.week_start_date,
        ws.weekly_avg_weight,
        ws.weekly_stddev_weight
    FROM weighin_base w
    JOIN weekly_stats ws
    ON w.customer_id = ws.customer_id AND DATE_TRUNC('week', w.weighin_date) = ws.week_start_date
),

-- Remove outliers semanais que fogem muito da média semanal
filtered_weekly_data AS (
    SELECT *
    FROM weighins_with_weekly_stats
    WHERE ABS(weight_kg - weekly_avg_weight) <= 1.5 * weekly_stddev_weight
),

-- Calcula estatísticas gerais usando IQR para identificação de outliers menos sensível a valores extremos
quartiles AS (
    SELECT
        customer_id,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY weight_kg) AS q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY weight_kg) AS q3
    FROM filtered_weekly_data
    GROUP BY customer_id
),

iqr_calculations AS (
    SELECT
        fwd.*,
        q.q1,
        q.q3,
        (q.q3 - q.q1) AS iqr,
        q.q1 - 1.0 * (q.q3 - q.q1) AS lower_bound,
        q.q3 + 1.0 * (q.q3 - q.q1) AS upper_bound
    FROM filtered_weekly_data fwd
    JOIN quartiles q ON fwd.customer_id = q.customer_id
),

-- Filtra os dados que estão dentro dos limites calculados pelo IQR
filtered_data AS (
    SELECT *
    FROM iqr_calculations
    WHERE weight_kg >= lower_bound AND weight_kg <= upper_bound
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

SELECT *
FROM fact_weighins
