INSERT INTO facts.fact_sales_orders (sale_order_key, order_key, customers_key, product_key, coupon_key, date_key, amount, price_per_unit, total, discount, fixed_total, status, created_at)
SELECT
    id as sale_order_key,
    dor.order_key,
    dcus.customers_key,
    dp.products_key as product_key,
    dc.coupon_key,
    TO_CHAR(dor.created_at, 'YYYYMMDD')::integer AS date_key,
    oi.amount,
    dp.price as price_per_unit,
    (oi.amount * dp.price) as total,
    dc.discount_percent as discount,
    (oi.amount * dp.price - ((oi.amount * dp.price) * COALESCE(dc.discount_percent, 0)/100)) fixed_total,
    status,
    created_at
FROM order_items as oi
LEFT JOIN
    dimensional.dim_products AS dp ON dp.products_key = oi.product_id
LEFT JOIN
    dimensional.dim_orders AS dor ON dor.order_key = oi.order_id
LEFT JOIN
    dimensional.dim_customers AS dcus ON dcus.customers_key = dor.customers_key
LEFT JOIN
    dimensional.dim_coupons AS dc ON dc.coupon_key = oi.coupon_id
ON CONFLICT (sale_order_key)
    DO UPDATE SET
                  order_key     = excluded.order_key,
                  customers_key = excluded.customers_key,
                  product_key = excluded.product_key,
                  coupon_key = excluded.coupon_key,
                  date_key = excluded.date_key,
                  amount = excluded.amount,
                  price_per_unit = excluded.price_per_unit,
                  total = excluded.total,
                  discount = excluded.discount,
                  fixed_total = excluded.fixed_total,
                  status = excluded.status,
                  created_at = excluded.created_at;