select employee_id, count(*) from {{ ref('obt_b') }} group by 1 having count(*) > 1
