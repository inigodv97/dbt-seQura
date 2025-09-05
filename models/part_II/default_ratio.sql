-- models/default_ratio.sql
{{ config(
    materialized = 'table'
) }}

/* Calculating total loans and total debt
    This CTE could potentially be an ephemeral in a separate file under an "intermediate" folder, 
    in case total_loans and total_debt were needed in other models */
WITH loans_debt AS (

    SELECT
        DATE_TRUNC(order_date, MONTH) AS month_year_order,
        -- Summing all the total loans, considering the ones that are in arreas and the ones that are not    
        SUM(current_order_value) AS total_loans,
        -- Summing only the loans that are in arreas
        SUM(
            CASE 
                WHEN is_in_default IS TRUE THEN (overdue_principal + overdue_fees) 
                ELSE 0 
            END
        ) as total_debt     
    FROM
        {{ref('orders')}}
    GROUP BY 
        month_year_order
)

SELECT
    month_year_order,
    total_debt,
    total_loans,     
    ROUND((total_debt / total_loans), 2) AS default_ratio
FROM 
    loans_debt