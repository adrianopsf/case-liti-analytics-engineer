WITH medicine_data AS (
    SELECT 
        medicine_id,
        name as medicine_name,
        tipo as medicine_type,
        label as medicine_label
    FROM {{ source('silver', 'silver_medicines') }}
),


medicine_prescription AS (
    SELECT 
        customer_id,
        medicine_id,
        description,
        dosage
    FROM {{ source('silver', 'silver_customer_medicines_prescriptions') }}
)


SELECT 
    md.medicine_id,
    md.medicine_name,
    md.medicine_type,
    md.medicine_label,
    mp.customer_id,
    mp.description,
    mp.dosage
 FROM medicine_data md
JOIN medicine_prescription mp ON md.medicine_id = mp.medicine_id
