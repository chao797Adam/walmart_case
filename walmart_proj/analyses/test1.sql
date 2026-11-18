select * from {{ source('walmart_databricks', 'orders') }} limit 10
