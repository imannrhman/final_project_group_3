DROP TABLE IF EXISTS presentations.daily_order_revenue_per_suppliers;
CREATE TABLE presentations.daily_order_revenue_per_suppliers
(
    date         DATE,
    supplier_name TEXT,
    country      TEXT,
    amount       BIGINT,
    revenue      TEXT
);

INSERT INTO presentations.daily_order_revenue_per_suppliers(date, supplier_name, country, amount, revenue)
SELECT
  dd.date,
  ds.name,
  ds.country,
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
	ON dp.suppliers_key = dp.suppliers_key
GROUP BY
	dd.date,  ds.suppliers_key;