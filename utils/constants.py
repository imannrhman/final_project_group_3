POSTGRES_DW_CONN = "postgres_dw"
DBT_EXECUTABLE_PATH="/home/airflow/.local/bin/dbt"
PROJECT_PATH="/opt/airflow"

FOLDER_SQL = "/opt/airflow/sql"
FOLDER_SQL_UPSERT = FOLDER_SQL + "/upsert/"
UPSERT_DIM_COUPONS_SQL = "upsert_dim_coupons.sql"
UPSERT_DIM_CUSTOMERS_SQL = "upsert_dim_customers.sql"
UPSERT_DIM_DATE_SQL = "upsert_dim_date.sql"
UPSERT_DIM_LOGIN_ATTEMPS_SQL = "upsert_dim_login_attemps.sql"
UPSERT_DIM_ORDERS_SQL = "upsert_dim_orders.sql"
UPSERT_DIM_PRODUCT_CATEGORIES_SQL = "upsert_dim_product_categories.sql"
UPSERT_DIM_PRODUCTS_SQL = "upsert_dim_products.sql"
UPSERT_DIM_SUPPLIERS_SQL = "upsert_dim_suppliers.sql"
UPSERT_FACT_SALES_ORDER_SQL = "upsert_fact_sales_orders.sql"
UPSERT_FACT_PRODUCT_PERFORMANCE = "upsert_fact_product_performance.sql"
UPSERT_FACT_PRODUCT_CATEGORY_PERFORMANCE = "upsert_fact_product_category_performance.sql"