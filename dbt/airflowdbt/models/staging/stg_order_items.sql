with

source as (

    select * from {{ source('raw', 'order_items') }}

),

renamed as (

    select
        
        id as order_items_id,
        
        order_id as orders_id,

        product_id as products_id,

        amount,

        coupon_id as coupons_id
        
    from source
)

select * from renamed