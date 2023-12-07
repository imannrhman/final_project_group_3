INSERT INTO dimensional.dim_coupons (coupon_key, discount_percent)
(SELECT id as coupon_key,
discount_percent
FROM coupuns)
ON CONFLICT (coupon_key)
DO UPDATE SET discount_percent = excluded.discount_percent;
