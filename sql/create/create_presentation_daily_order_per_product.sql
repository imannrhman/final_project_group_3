DROP TABLE IF EXISTS presentations.daily_order_revenue_per_product;
CREATE TABLE presentations.daily_order_revenue_per_product
(
    date         DATE,
    product_name TEXT,
    category   TEXT,
    suppliers    TEXT,
    amount       BIGINT,
    revenue      TEXT
);


INSERT INTO presentations.daily_order_revenue_per_product(date, product_name, category, suppliers, amount, revenue)
SELECT
  dd.date,
  dp.name as product_name,
  dpc.name as category,
  ds.name as suppliers,
  SUM(fso.amount) as amount,
  SUM(fso.fixed_total) AS revenue
FROM
	facts.fact_sales_orders as fso
JOIN
	dimensional.dim_date as dd
	ON fso.date_key = dd.date_key
JOIN
	dimensional.dim_products as dp
	ON fso.product_key = dp.products_key
JOIN
	dimensional.dim_suppliers as ds
	ON dp.suppliers_key = ds.suppliers_key
JOIN
    dimensional.dim_product_categories as dpc
    ON dp.product_categories_key = dpc.product_categories_key
GROUP BY
	dd.date, dp.products_key, dpc.product_categories_key, ds.suppliers_key;