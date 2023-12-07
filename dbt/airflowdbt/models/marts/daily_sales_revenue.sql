with sales as (
    select * from {{ ref('fct_sales_order') }}
),

dates as (

    select * from {{ ref('dim_date') }}
),

join_daily_order as (
    select
        dates.date_day,
        count(sales.orders_id) as total_sales,
        sum(sales.revenue) as total_revenue,
        count(*) filter (where sales.using_coupon) as total_coupon_used
    from sales
    left join dates on dates.date_day = sales.order_date
    group by 1
    order by 1
)
select * from join_daily_order
