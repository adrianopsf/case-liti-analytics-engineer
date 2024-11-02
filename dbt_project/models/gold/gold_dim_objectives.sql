WITH objectives_data AS (
    SELECT 
        objectives_id,
        description,
        tipo
    FROM {{ source('silver', 'silver_objectives') }}
),


customer_objectives AS (
    SELECT 
        customer_id,
        objective as objectives_id,
        start_date,
        updated_at
    FROM {{ source('silver', 'silver_customer_objectives') }}
)


SELECT 
    od.objectives_id,
    od.description,
    od.tipo,
    co.customer_id,
    co.start_date,
    co.updated_at
 FROM objectives_data od
JOIN customer_objectives co ON od.objectives_id = co.objectives_id
