INSERT INTO dimensional.dim_orders (order_key, customers_key, status, created_at)
(SELECT
         id as order_key,
         CAST (customer_id AS BIGINT) as customers_key,
         status,
         created_at as DATE
     FROM orders)
ON CONFLICT (order_key)
    DO UPDATE SET
                  status     = excluded.status,
                  created_at = excluded.created_at;
