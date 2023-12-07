with orders as (
    select * from {{ ref('fct_orders') }}
),

dates as (

    select * from {{ ref('dim_date') }}
),

join_order as (
    select
        dates.date_day,
        count(*) filter (where orders.using_coupon) as total_coupon_used,
        count(orders.total_order) as total_order
    from orders
    left join dates on dates.date_day = orders.order_date
    group by 1
    order by 1
),

get_percentage as (
    select
        join_order.*,
        cast(((cast(join_order.total_coupon_used as decimal(7,2))/ cast(join_order.total_order as decimal(7,2))) * 100)
            as decimal(7,2))
            as percentage_coupon_used
    from join_order
)
select * from get_percentage