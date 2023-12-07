with coupon as (
    
    select * from {{ ref('stg_coupons') }}

),


new_coupons as (
    select
    
    coupon.coupons_id,
    coupon.discount_percent

    from coupon
)

select * from new_coupons





