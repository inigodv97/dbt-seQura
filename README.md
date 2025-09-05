# dbt-seQura challenge

## PART I - Data Extraction (SQL)

Calculating shopper’s recurrence rate for each month and merchant, as explained in the exercise.
The provided `orders_merchant.csv` CSV file was split into **two seed tables** which were later used in the model.

The model was developed and executed in a **dbt Cloud project** connected to **BigQuery**.  
All modeling and development work was carried out locally using **Visual Studio Code** together with the **dbt CLI**

Inside the `models/part_I` folder you will find a CSV of the output given by the model, named **`model_output.csv`**. 

## PART II - Data Modeling (DBT)

This project contains two main dbt models that together provide both granular and aggregated views of loan performance.

---

### 1. `unbalanced_orders`

The **Unbalanced Orders** table contains the core calculations requested in the exercise.  
It provides order-level granularity, and the desired output requested in the exercise:

- shopper_age
- month_year_order (YYYY-MM)
- product
- merchant
- default_type
- delayed_period  
- (extra) Boolean flags for overdue thresholds (`is_in_period_17`, `is_in_period_30`, etc.)  

This table serves as the detailed foundation for any further aggregation or monitoring.  

⚠️ **Note:** The Default Ratio is *not* included here. Adding it at the order level would result in duplication, since each order in the same month would carry the same ratio.

---

### 2. `default_ratio`

The **Default Ratio** table is designed specifically for the stakeholders’ request:  
to monitor how the **Default Ratio evolves month by month**.

It provides **monthly granularity**, with the following metrics:

- `total_loans`: Sum of all loans issued in a given month (both defaulted and non-defaulted)  
- `total_debt`: Sum of overdue principal and fees for defaulted loans  
- `default_ratio`: The key metric, calculated as `total_debt / total_loans`  

This structure avoids duplication and ensures that the ratio is only computed once per month, which makes it stakeholder-friendly for reporting and dashboarding.

---

## 3. Design considerations

Inside the `default_ratio` model, there is a CTE (`loans_debt`) that performs the aggregation.  
In principle, this logic could be extracted into a **separate ephemeral model** if other models needed to reuse it.  

However, there is always a trade-off between:

- **More files / modularity** → better reusability but more complex project structure  
- **Compact models** → easier to follow in isolation but with potential repeated calculations  

For this exercise, the priority was **clarity**, so the logic was kept **within the `default_ratio` model**.
