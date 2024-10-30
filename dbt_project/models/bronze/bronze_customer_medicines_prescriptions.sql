WITH raw_customer_medicines_prescriptions AS (
  SELECT * FROM {{ source('bronze', 'customer_medicines_prescriptions') }}
)

SELECT * FROM raw_customer_medicines_prescriptions