select * from {{ source('walmart_databricks', 'employees') }} limit 10
