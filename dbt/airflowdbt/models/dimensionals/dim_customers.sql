with customer as (
    
    select * from {{ ref('stg_customers') }}

),


new_customer as (
    select
    
    customer.customers_id,
    concat(customer.first_name,' ', customer.last_name) as full_name,
    customer.gender,
    customer.address,
    customer.zip_code

    from customer
)

select * from new_customer


