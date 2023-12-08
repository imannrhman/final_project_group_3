--- orders in returned analytics
with orders as (
    select * from {{ ref('fct_orders') }}
),

join_order as (
    select
       orders.*
    from orders
    where orders.status in ('RETURNED')
)
select * from join_order