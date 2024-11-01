WITH casted_data AS (
    SELECT 
        CAST("CustomerId" AS VARCHAR) AS customer_id,
        CAST("isActive" AS BOOLEAN) AS is_active,
        CAST("isActivePaid" AS BOOLEAN) AS is_active_paid,
        CAST(NULLIF("customerBirthDate", '') AS DATE) AS customer_birth_date,
        CAST(NULLIF("firstStartDate", '') AS DATE) AS first_start_date,
        CAST(NULLIF("acquiredDate", '') AS DATE) AS acquired_date,
        CAST(NULLIF("firstPaymentDate", '') AS DATE) AS first_payment_date,
        CAST(NULLIF("lastChargePaidDate", '') AS DATE) AS last_charge_paid_date,
        CAST(NULLIF("churnDate", '') AS DATE) AS churn_date,
        CAST(NULLIF("customerCreatedAt", '') AS TIMESTAMP) AS customer_created_at,
        CAST(NULLIF("customerHeight", '') AS FLOAT) AS customer_height,
        CAST("activePlan" AS VARCHAR) AS active_plan,
        CAST("customerPlan" AS VARCHAR) AS customer_plan,
        CAST("customerGender" AS VARCHAR) AS customer_gender,
        CAST("originChannelGroup" AS VARCHAR) AS origin_channel_group,
        CAST("customerDoctor" AS VARCHAR) AS customer_doctor,
        CAST("customerNutritionist" AS VARCHAR) AS customer_nutritionist,
        CAST("customerBesci" AS VARCHAR) AS customer_besci,
        CAST("customerInOnboarding" AS VARCHAR) AS customer_in_onboarding      
    FROM {{ source('bronze', 'bronze_customer') }}
),

base_data AS (
    SELECT 
        customer_id,
        
        /* Limpeza e normalização de gênero */
        CASE 
            WHEN customer_gender IS NULL THEN 'not specified' 
            ELSE LOWER(customer_gender) 
        END AS customer_gender,
        
        /* Tratamento para faixa de altura aceitável do usuário */
        CASE 
             WHEN customer_height BETWEEN 50 AND 300 THEN customer_height
             ELSE NULL 
        END AS customer_height,
       
        /* Tratamento para a data de aniversário não ser data futura */
        CASE 
            WHEN CAST(customer_birth_date AS DATE) <= CURRENT_DATE THEN CAST(customer_birth_date AS DATE)
            ELSE NULL
        END AS customer_birth_date,

        /* Padronização do canal de origem */
        
        CASE 
            WHEN origin_channel_group IS NULL THEN 'not specified' 
            ELSE LOWER(origin_channel_group) 
        END AS origin_channel_group,
        
        /* Padronização do plano do usuário */
        CASE 
            WHEN customer_plan IS NULL THEN 'not specified' 
            ELSE LOWER(customer_plan) 
        END AS customer_plan,
        
        
        COALESCE(active_plan, 'false') AS active_plan,
        is_active,
        is_active_paid,

        /* Assegurar que as datas não sejam datas futuras */
        CASE 
            WHEN first_start_date <= CURRENT_DATE THEN first_start_date
            ELSE NULL
        END AS first_start_date,

        CASE 
            WHEN acquired_date <= CURRENT_DATE THEN acquired_date
            ELSE NULL
        END AS acquired_date,

        CASE 
            WHEN first_payment_date <= CURRENT_DATE THEN first_payment_date
            ELSE NULL
        END AS first_payment_date,

        CASE 
            WHEN last_charge_paid_date <= CURRENT_DATE THEN last_charge_paid_date
            ELSE NULL
        END AS last_charge_paid_date,

        CASE 
            WHEN churn_date <= CURRENT_DATE THEN churn_date
            ELSE NULL
        END AS churn_date,

        CASE 
            WHEN customer_created_at <= CURRENT_TIMESTAMP THEN customer_created_at
            ELSE NULL
        END AS customer_created_at,

        /* Indicador de churn */
        CASE 
            WHEN churn_date IS NOT NULL THEN 'churn'
            ELSE 'ativo'
        END AS churn_status,

        /* Campos de relacionamento tratados como nulos onde apropriado */
        COALESCE(customer_doctor, 'desconhecido') AS customer_doctor,
        COALESCE(customer_nutritionist, 'desconhecido') AS customer_nutritionist,
        COALESCE(customer_besci, 'desconhecido') AS customer_besci

    FROM casted_data
),

-- Tratamento para remover duplicatas
deduped_data AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY customer_created_at DESC) AS row_num
    FROM base_data
)

-- Seleção dos dados finais, mantendo apenas uma linha por CustomerId
SELECT 
    customer_id,
    customer_gender,
    customer_height,
    customer_birth_date,
    origin_channel_group,
    customer_plan,
    active_plan,
    is_active,
    is_active_paid,
    first_start_date,
    acquired_date,
    first_payment_date,
    last_charge_paid_date,
    churn_date,
    customer_created_at,
    churn_status,
    customer_doctor,
    customer_nutritionist,
    customer_besci
FROM deduped_data
WHERE row_num = 1
