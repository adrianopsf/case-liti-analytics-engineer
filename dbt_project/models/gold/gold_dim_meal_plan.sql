WITH meal_plan_data AS (
    SELECT 
        meal_plan_id,
        name as meal_plan_name,
        group_name as meal_plan_group,
        created_at
    FROM {{ source('silver', 'silver_meal_plans') }}
),


customer_meal_plan AS (
    SELECT 
        customer_id,
        meal_plan_id,
        restrictions_vegan,
        restrictions_gluten,
        restrictions_lactose,
        restrictions_ovolacto,
        restrictions_high_fod_maps,
        start_date,
        end_date
    FROM {{ source('silver', 'silver_customer_meal_plan') }}
)


SELECT 
    mp.meal_plan_id,
    mp.meal_plan_name,
    mp.meal_plan_group,
    cm.customer_id,
    cm.restrictions_vegan,
    cm.restrictions_gluten,
    cm.restrictions_lactose,
    cm.restrictions_ovolacto,
    cm.restrictions_high_fod_maps,
    cm.start_date,
    cm.end_date
 FROM meal_plan_data mp
JOIN customer_meal_plan cm ON mp.meal_plan_id = cm.meal_plan_id