-- models/shopper_recurrence_rate.sql
{{ config(
    materialized = 'table'
) }}

/*
Clean and prepare the orders table
    Parse dates from string to DATE
    Truncate to month for grouping
    Create a shopper_merchant_id (surrogate key) to track shopper activity per merchant */

WITH orders_cleaned AS (
    
    SELECT
        order_id,
        shopper_id,
        merchant_id,
        -- Ideally we’d use dbt_utils.surrogate_key function to generate a hash, but here I build it manually to avoid package dependencies from your side 
        CONCAT(CAST(shopper_id AS STRING), '_', CAST(merchant_id AS STRING)) AS shopper_merchant_id,
        DATE_TRUNC(PARSE_DATE('%d/%m/%y', order_date), MONTH) AS order_month
    FROM 
        {{ ref('orders') }} AS orders

),

-- Identify the most recent *closed* month (i.e. the last full month with complete data, excluding the ongoing one)
last_closed AS (
    
    SELECT
        MAX(
            DATE_SUB(order_month, INTERVAL 1 MONTH)
        ) AS last_closed_month
    FROM
        orders_cleaned
),

-- Calculate recurrent shoppers
recurrent_shoppers AS (

    SELECT 
        COUNT(*) AS n_recurrent_shoppers,
        merchant_id,
        order_month
    FROM ( 
    
        SELECT
            merchant_id,
            order_month,
            /* A shopper is considered recurrent if:
                (a) They purchased in the closed month under analysis
                (b) They had a previous purchase with the same merchant
                (c) That previous purchase happened within the last 12 months */
            CASE
                WHEN LAG(order_month) OVER (PARTITION BY shopper_merchant_id ORDER BY order_month) IS NOT NULL 
                    AND LAG(order_month) OVER (PARTITION BY shopper_merchant_id ORDER BY order_month) != order_month -- Ignore multiple purchases within the same month
                    AND LAG(order_month) OVER (PARTITION BY shopper_merchant_id ORDER BY order_month) > DATE_SUB(order_month, INTERVAL 12 MONTH)   
                    THEN TRUE
                ELSE FALSE
            END AS is_recurrent
        FROM
            orders_cleaned o

    )
    WHERE is_recurrent IS TRUE
    GROUP BY merchant_id, order_month

),

-- Calculating the total number of shoppers per merchant and closed month
total_shoppers AS (
    
    SELECT
        merchant_id,
        order_month,
        COUNT(DISTINCT shopper_id) AS n_total_shoppers
    FROM
        orders_cleaned
    WHERE order_month <= (SELECT last_closed_month FROM last_closed)
    GROUP BY merchant_id, order_month
    ORDER BY merchant_id, order_month
)

-- Recurrence rate per merchant and month: Formula = (recurrent shoppers / total shoppers) * 100

SELECT
    t.order_month,
    merchant_name,
    COALESCE(CAST(100 * (m.n_recurrent_shoppers / t.n_total_shoppers) AS INTEGER), 0) AS recurrence_rate
FROM total_shoppers AS t
LEFT JOIN recurrent_shoppers AS m
    ON t.order_month = m.order_month AND t.merchant_id = m.merchant_id
LEFT JOIN 
    {{ ref('merchants') }} AS merchants
    ON t.merchant_id = merchants.merchant_id
ORDER BY merchant_name, t.order_month
