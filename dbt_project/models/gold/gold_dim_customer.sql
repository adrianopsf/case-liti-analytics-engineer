WITH customer_data AS (
    SELECT 
        customer_id,
        customer_gender,
        customer_birth_date,
        customer_height,
        origin_channel_group as customer_origin_channel_group,
        is_active,
        is_active_paid,
        age as customer_age
        
    FROM {{ source('silver', 'silver_customer') }}
)

SELECT * FROM customer_data
