with customers as (

    select * from {{ ref('dim_customers') }}

),
login_attemps as(
    select * from {{ ref('fct_login_attemps') }}
),

orders as (
     select * from {{ ref('fct_orders') }}
),

sales as (
     select * from {{ ref('fct_sales_order') }}
),

user_order as (
    select
        customers.customers_id,
        customers.full_name,
        count(orders.orders_id) as total_orders,
        sum(sales.revenue) as total_spent
    from customers
    left join orders on customers.customers_id = orders.customers_id
    left join sales on customers.customers_id = sales.customers_id
    group by 1,2
),

user_activity_login as (
    select
        user_order.*,
        login_attemps.login_success,
        login_attemps.login_failed,
        login_attemps.last_login_attemps
    from user_order
    left join login_attemps on user_order.customers_id = login_attemps.customers_id
)

select * from user_activity_login