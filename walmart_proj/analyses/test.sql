select * from {{ source('walmart_databricks', 'customers') }} limit 10
