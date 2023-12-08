-- show returned order per date
with orders as (
    select * from {{ ref('fct_orders') }}
),

dates as (

    select * from {{ ref('dim_date') }}
),

join_daily_order as (
    select
        dates.date_day,
        orders.status,
        sum(orders.total_amount) as total_amount,
        sum(orders.fix_total_purchase) as total_purchase,
        count(orders.orders_id) as total_orders,
        count(*) filter (where orders.using_coupon) as total_coupon_used
    from orders
    left join dates on dates.date_day = orders.order_date
    where orders.status = 'RETURNED'
    group by 1,2
    order by 1
)
select * from join_daily_order