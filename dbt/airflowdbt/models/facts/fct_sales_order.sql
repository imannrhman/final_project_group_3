with sales as (

    select
        sales.orders_id,
        sales.status,
        sales.customers_id,
        sales.customer_name,
        sales.customer_gender,
        sales.order_address,
        sales.order_zip_code,
        sales.total_amount,
        sales.using_coupon,
        sales.fix_total_purchase as revenue,
        sales.order_date

    from {{ ref('fct_orders') }} as sales where sales.status in ('SENT', 'FINISHED', 'RECEIVED')
)

select * from sales