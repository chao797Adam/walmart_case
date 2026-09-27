with
    a as (select count(*) as c from {{ ref('orders_t') }}),
    b as (select count(*) as c from {{ ref('fact_orders') }})
select *
from a, b
where a.c != b.c
