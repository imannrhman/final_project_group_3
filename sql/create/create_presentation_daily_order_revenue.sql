DROP TABLE IF EXISTS presentations.daily_order_revenue;
CREATE TABLE presentations.daily_order_revenue
(
    date        DATE PRIMARY KEY ,
    amount      BIGINT,
    revenue     TEXT
);

INSERT INTO presentations.daily_order_revenue (date, amount, revenue)
SELECT
  dd.date,
  SUM(fso.amount) as amount,
  SUM(fso.fixed_total) AS revenue
FROM
	facts.fact_sales_orders as fso
JOIN
	dimensional.dim_date as dd
	ON fso.date_key = dd.date_key
GROUP BY
	dd.date
ON CONFLICT (date)
    DO UPDATE SET
                  amount  = excluded.amount,
                  revenue = excluded.revenue;
