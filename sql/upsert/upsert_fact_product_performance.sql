INSERT INTO facts.fact_product_performance(product_key, name, count_of_sales, total_revenue)
SELECT
	dp.id as product_key,
	MAX(dp.name) as name,
	sum(foi.amount) as count_of_sales,
	sum(foi.fixed_total) as total_revenue
FROM products dp
LEFT JOIN facts.fact_sales_orders foi
	ON foi.product_key = product_key
GROUP BY dp.id
ON CONFLICT (product_key)
    DO UPDATE SET
                  product_key = excluded.product_key,
                  name = excluded.name,
                  count_of_sales = excluded.count_of_sales,
                  total_revenue = excluded.total_revenue;