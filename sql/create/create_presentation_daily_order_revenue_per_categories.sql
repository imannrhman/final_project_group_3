DROP TABLE IF EXISTS presentations.daily_order_revenue_per_categories;
CREATE TABLE presentations.daily_order_revenue_per_categories
(
    date         DATE,
    category     TEXT,
    amount       BIGINT,
    revenue      TEXT
);

INSERT INTO presentations.daily_order_revenue_per_categories(date, category, amount, revenue)
SELECT
  dd.date,
  dpc.name as category,
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
	dimensional.dim_product_categories as dpc
	ON dpc.product_categories_key = dp.product_categories_key
GROUP BY
	dd.date,  dpc.product_categories_key;