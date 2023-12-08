with orders as (
    select * from {{ ref('fct_orders') }}
),

orders_product as (
    select
        *
    from {{ ref('fct_order_products') }} as orders_product

),

dates as (
    select * from {{ ref('dim_date') }}
),

join_order as (
    select
        orders.orders_id,
        sum(orders_product.amount) as total_amount,
        mode() within group (order by orders_product.product_name) as most_product_buy,
        mode() within group (order by orders_product.products_id) as most_product_buy_id,
        max(orders.order_date) as order_date
    from orders
    left join orders_product on orders_product.orders_id = orders.orders_id
    where orders_product.orders_id is not null
    group by 1
    order by 1
),

joined_daily_order as (
    select
        dates.date_day,
        sum(join_order.total_amount) as total_amount,
        count(*) as total_orders,
        mode() within group (order by join_order.most_product_buy) as most_product_buy,
        mode() within group (order by join_order.most_product_buy_id) as most_product_buy_id
    from join_order
    left join dates on dates.date_day = join_order.order_date
    group by dates.date_day
)

select * from joined_daily_order

