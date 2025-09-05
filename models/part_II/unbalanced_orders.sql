-- models/unbalanced_orders.sql
{{ config(
    materialized = 'table'
) }}

-- Cleaning up orders table, such as extracting year month from order_date and taking only the fields we need
WITH orders_cleaned AS (
    
    SELECT
        order_id,
        shopper_id,
        DATE_TRUNC(order_date, MONTH) AS month_year_order,         
        product_id,        
        merchant_id,
        is_in_default,
        days_unbalanced,
        current_order_value,
        overdue_principal,
        overdue_fees        
    FROM
        {{ref('orders')}} 
)

SELECT 
    age AS shopper_age,
    orders.month_year_order,
    product_id AS product,
    merchant_id AS merchant,
    default_type_id AS default_type,
    days_unbalanced,
    /* Delayed period will take the closest period in which the loan is in arreas 
        (i.e. if days_unbalanced of an order is 35, then delayed_period will be 30) */
    CASE
        WHEN days_unbalanced >= 17 AND days_unbalanced < 30 THEN 17
        WHEN days_unbalanced >= 30 AND days_unbalanced < 60 THEN 30
        WHEN days_unbalanced >= 60 AND days_unbalanced < 90 THEN 60
        WHEN days_unbalanced >= 90 THEN 90
        ELSE NULL
    END AS delayed_period,
    /* Here we separate the periods in buckets
        That way we have separate Booleans stating how many periods has the loan been in arreas 
        (i.e. if days_unbalanced in 35, then this order will be included in "is_in_period_17" & "is_in_period_30" ) */
    CASE WHEN days_unbalanced >= 17 THEN TRUE ELSE FALSE END AS is_in_period_17,
    CASE WHEN days_unbalanced >= 30 THEN TRUE ELSE FALSE END AS is_in_period_30,
    CASE WHEN days_unbalanced >= 60 THEN TRUE ELSE FALSE END AS is_in_period_60,
    CASE WHEN days_unbalanced >= 90 THEN TRUE ELSE FALSE END AS is_in_period_90
FROM  
    orders_cleaned AS orders
LEFT JOIN 
    {{ref('dim_shoppers')}} shoppers
    ON orders.shopper_id = shoppers.shopper_id
LEFT JOIN 
    {{ref('rel_default_order_type')}} order_type
    ON orders.order_id = order_type.order_id
