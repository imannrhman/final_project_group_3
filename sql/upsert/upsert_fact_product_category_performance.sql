INSERT INTO facts.fact_product_category_performance(category_key, name, count_of_sales, total_revenue)
SELECT
	dc.product_categories_key as category_key,
	MAX(dc.name) as name,
	sum(foi.amount) as count_of_sales,
	sum(foi.fixed_total) as total_revenue
FROM dimensional.dim_product_categories dc
LEFT JOIN dimensional.dim_products as dp
    ON dp.product_categories_key = dc.product_categories_key
LEFT JOIN facts.fact_sales_orders foi
	ON foi.product_key = dp.products_key
GROUP BY dc.product_categories_key
ORDER BY total_revenue DESC, count_of_sales DESC
ON CONFLICT (category_key)
    DO UPDATE SET
                  category_key = excluded.category_key,
                  name = excluded.name,
                  count_of_sales = excluded.count_of_sales,
                  total_revenue = excluded.total_revenue;