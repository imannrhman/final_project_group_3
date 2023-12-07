with orders as (

    select
        orders.orders_id,
        orders.status,
        orders.customers_id,
        date(orders.created_at) as order_date
    from {{ ref('stg_orders') }} as orders

),

order_items as (

    select
        order_items.orders_id,
        count( order_items.orders_id) as total_order,
        sum(order_items.amount) as total_amount,
        sum(order_items.total) as total_before_discount,
        sum(order_items.fix_total) as fix_total,
        (case
            when count(*) filter (where order_items.use_coupons) > 0 then
                true
            else
                false
            end
        ) as using_coupon,
        count(*) filter (where order_items.use_coupons) as coupon_using
        from {{ ref('fct_order_products') }} as order_items
    group by 1

),

customers as (
    select * from {{ ref('dim_customers') }}
),


join_order as (

     select
        orders.orders_id,
        orders.status,
        customers.customers_id,
        customers.full_name as customer_name,
        customers.gender as customer_gender,
        customers.address as order_address,
        customers.zip_code as order_zip_code,
        order_items.total_order,
        order_items.total_amount,
        order_items.total_before_discount as total_purchase,
        order_items.using_coupon,
        order_items.coupon_using,
        order_items.fix_total as fix_total_purchase,
        orders.order_date
     from orders
     left join order_items on orders.orders_id = order_items.orders_id
     left join customers on orders.customers_id = customers.customers_id

)

select * from join_order

