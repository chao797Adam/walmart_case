with
    a as (select sum(total_amount) as c from {{ ref('fact_orders') }}),
    b as (select sum(line_amount) as c from {{ ref('fact_order_items') }})
select *
from a, b
where abs(a.c - b.c) > 0.01
