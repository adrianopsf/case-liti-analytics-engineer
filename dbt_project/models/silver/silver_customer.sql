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
        CASE LOWER(customer_gender)
            WHEN 'male' THEN 'Male'
            WHEN 'female' THEN 'Female'
            WHEN 'other' THEN 'Other'
            ELSE 'Not specified'
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

        /* Padronização e categorização do canal de origem */
        CASE origin_channel_group
            WHEN 'other' THEN 'Outro'
            WHEN 'projeto_video' THEN 'Projeto Video'
            ELSE origin_channel_group
        END AS origin_channel_group,

        /* Normalização e tratamento do plano */
        COALESCE(customer_plan, 'Sem Plano') AS customer_plan,
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
            WHEN churn_date IS NOT NULL THEN 'Churn'
            ELSE 'Ativo'
        END AS churn_status,

        /* Campos de relacionamento tratados como nulos onde apropriado */
        COALESCE(customer_doctor, 'Desconhecido') AS customer_doctor,
        COALESCE(customer_nutritionist, 'Desconhecido') AS customer_nutritionist,
        COALESCE(customer_besci, 'Desconhecido') AS customer_besci

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
SELECT *
FROM deduped_data
WHERE row_num = 1
