with orders as (
    select * from {{ ref('fct_orders') }}
),

join_daily_order as (
    select
        orders.status,
        count(orders.orders_id) as total_orders,
        sum(orders.total_amount) as total_amount,
        sum(orders.fix_total_purchase) as total_purchase,
        count(*) filter (where orders.using_coupon) as total_coupon_used
    from orders
    group by 1
    order by 1
)
select * from join_daily_order