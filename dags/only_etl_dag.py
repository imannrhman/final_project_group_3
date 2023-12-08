import pandas as pd
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

#Local
from utils import arguments, converters, constants
from plugins.operators.fetch_local_data_operators import FetchLocalDataOperator
from plugins.operators.clean_dataframe_operators import CleanDataframeOperator, ChangeType
from plugins.operators.insert_database_operators import InsertDataframeToPostgresOperator

#Define constant variabel of Tasks
TASK_FETCH_COUPONS="fetch_coupons_data"
TASK_FETCH_CUSTOMERS="fetch_customers_data"
TASK_FETCH_LOGIN_ATTEMPS="fetch_login_attemps_data"
TASK_FETCH_ORDER_ITEMS="fetch_order_items_data"
TASK_FETCH_ORDERS="fetch_orders_data"
TASK_FETCH_PRODUCT_CATEGORIES="fetch_product_categories_data"
TASK_FETCH_PRODUCTS="fetch_products_data"
TASK_FETCH_SUPPLIERS="fetch_suppliers_data"


TASK_CLEAN_COUPONS="clean_coupons_data"
TASK_CLEAN_CUSTOMERS="clean_customers_data"
TASK_CLEAN_LOGIN_ATTEMPS="clean_login_attemps_data"
TASK_CLEAN_ORDER_ITEMS="clean_order_items_data"
TASK_CLEAN_ORDERS="clean_orders_data"
TASK_CLEAN_PRODUCT_CATEGORIES="clean_product_categories_data"
TASK_CLEAN_PRODUCTS="clean_products_data"
TASK_CLEAN_SUPPLIERS="clean_suppliers_data"

TASK_INSERT_COUPONS="insert_coupons_data"
TASK_INSERT_CUSTOMERS="insert_customers_data"
TASK_INSERT_LOGIN_ATTEMPS="insert_login_attemps_data"
TASK_INSERT_ORDER_ITEMS="insert_order_items_data"
TASK_INSERT_ORDERS="insert_orders_data"
TASK_INSERT_PRODUCT_CATEGORIES="insert_product_categories_data"
TASK_INSERT_PRODUCTS="insert_products_data"
TASK_INSERT_SUPPLIERS="insert_suppliers_data"

TASK_GROUP_FETCH_DATA_JSON = "fetch_data_json"
TASK_GROUP_FETCH_DATA_CSV = "fetch_data_csv"
TASK_GROUP_FETCH_DATA_EXCEL = "fetch_data_excel"
TASK_GROUP_FETCH_DATA_AVRO = "fetch_data_avro"
TASK_GROUP_FETCH_DATA_PARQUET = "fetch_data_parquet"


with DAG(
    "only_etl_dag",
    start_date=days_ago(1),
    schedule_interval=None,
    default_args=arguments.default,
    catchup=False,
    concurrency=8,
    max_active_runs=1
) as dag:

    # Data Ingestion Task Group
    with TaskGroup("data_ingestion", tooltip="Data Ingestion") as data_ingestion:
        with TaskGroup(TASK_GROUP_FETCH_DATA_JSON, tooltip="Fetch Data from JSON") as fetch_data_json:
            fetch_coupuns = FetchLocalDataOperator(
                task_id=TASK_FETCH_COUPONS,
                sources_data= ['data/coupons.json'], 
                converter=converters.json_to_pandas)
            
            fetch_login_attemps = FetchLocalDataOperator(
                task_id=TASK_FETCH_LOGIN_ATTEMPS,
                sources_data=[
                    'data/login_attempts_0.json', #file path disesuaikan
                    'data/login_attempts_1.json',
                    'data/login_attempts_2.json',
                    'data/login_attempts_3.json',
                    'data/login_attempts_4.json',
                    'data/login_attempts_5.json',
                    'data/login_attempts_6.json',
                    'data/login_attempts_7.json',
                    'data/login_attempts_8.json',
                    'data/login_attempts_9.json'
                ],
                converter=converters.json_to_pandas)
        
        with TaskGroup(TASK_GROUP_FETCH_DATA_CSV, tooltip="Fetch Data from CSV") as fetch_data_csv:
            fetch_customers = FetchLocalDataOperator(
                task_id=TASK_FETCH_CUSTOMERS,
                sources_data=[
                    'data/customer_0.csv', #file path disesuaikan
                    'data/customer_1.csv',
                    'data/customer_2.csv',
                    'data/customer_3.csv',
                    'data/customer_4.csv',
                    'data/customer_5.csv',
                    'data/customer_6.csv',
                    'data/customer_7.csv',
                    'data/customer_8.csv',
                    'data/customer_9.csv'
                ],
                converter=converters.csv_to_pandas)
        
        with TaskGroup(TASK_GROUP_FETCH_DATA_EXCEL, tooltip="Fetch Data from Excel") as fetch_data_excel:
            fetch_product_categories = FetchLocalDataOperator(
                task_id=TASK_FETCH_PRODUCT_CATEGORIES,
                sources_data=[
                    'data/product_category.xls'
                ],
                converter=converters.excel_to_pandas,)
        
            fetch_products = FetchLocalDataOperator(
                task_id=TASK_FETCH_PRODUCTS,
                sources_data=[
                    'data/product.xls'
                ],
                converter=converters.excel_to_pandas,)
        
            fetch_suppliers= FetchLocalDataOperator(
                task_id=TASK_FETCH_SUPPLIERS,
                sources_data=[
                    'data/supplier.xls'
                ],
                converter=converters.excel_to_pandas,)
        
        with TaskGroup(TASK_GROUP_FETCH_DATA_AVRO, tooltip="Fetch Data from Avro") as fetch_data_avro:
            fetch_order_items = FetchLocalDataOperator(
                task_id=TASK_FETCH_ORDER_ITEMS,
                sources_data=[
                    'data/order_item.avro'
                ],
                converter=converters.avro_to_pandas,)
        
        with TaskGroup(TASK_GROUP_FETCH_DATA_PARQUET, tooltip="Fetch Data from Parquet") as fetch_data_parquet:
            fetch_orders = FetchLocalDataOperator(
                task_id=TASK_FETCH_ORDERS,
                sources_data=[
                    'data/order.parquet'
                ],
                converter=converters.parquet_to_pandas,)
        
        fetch_data_json >> fetch_data_csv >> fetch_data_excel >> fetch_data_avro >> fetch_data_parquet
    
    # Data Transform Task Group
    with TaskGroup("data_transform", tooltip="Data Transform") as data_transform:
        
        # Data Transform and Load for Coupons in Task Group
        with TaskGroup("transform_coupons", tooltip="Data Transform Coupons") as transform_coupon:
            clean_coupuns = CleanDataframeOperator(
                task_id=TASK_CLEAN_COUPONS,
                dataframe=data_ingestion.get_child_by_label(TASK_GROUP_FETCH_DATA_JSON).get_child_by_label(TASK_FETCH_COUPONS).output,
                drop_duplicates_columns=['id'],
                change_type_columns=[
                    ChangeType(column_name="discount_percent", new_type=float),
            ])

            insert_coupons = InsertDataframeToPostgresOperator(
                task_id=TASK_INSERT_COUPONS,
                dataframe=clean_coupuns.output,
                table_name='coupons',
                conn_id=constants.POSTGRES_DW_CONN,
            )

            clean_coupuns >> insert_coupons
    
        # Data Transform and Load for Customers in Task Group
        with TaskGroup("transform_customers", tooltip="Data Transform Customers") as transform_customers:
            clean_customers = CleanDataframeOperator(
                task_id=TASK_CLEAN_CUSTOMERS,
                dataframe=data_ingestion.get_child_by_label(TASK_GROUP_FETCH_DATA_CSV).get_child_by_label(TASK_FETCH_CUSTOMERS).output,
                drop_columns=['Unnamed: 0'],
                drop_duplicates_columns=['id'],
                change_type_columns=[
                    ChangeType(column_name="zip_code", new_type=str),
                ])
            
            insert_customers = InsertDataframeToPostgresOperator(
                task_id=TASK_INSERT_CUSTOMERS,
                dataframe=clean_customers.output,
                table_name='customers',
                conn_id=constants.POSTGRES_DW_CONN,
            )

            clean_customers >> insert_customers
        
        # Data Transform and Load for Login Attemps in Task Group
        with TaskGroup("transform_login_attemps", tooltip="Data Transform Login Attemps") as transform_login_attemps:
            clean_login_attemps= CleanDataframeOperator(
                task_id=TASK_CLEAN_LOGIN_ATTEMPS,
                dataframe=data_ingestion.get_child_by_label(TASK_GROUP_FETCH_DATA_JSON).get_child_by_label(TASK_FETCH_LOGIN_ATTEMPS).output,
                drop_duplicates_columns=['id'])
            
            insert_login_attemps= InsertDataframeToPostgresOperator(
                task_id=TASK_INSERT_LOGIN_ATTEMPS,
                dataframe=clean_login_attemps.output,
                table_name='login_attemps',
                conn_id=constants.POSTGRES_DW_CONN,
            )
         
            clean_login_attemps >> insert_login_attemps


        # Data Transform and Load for Order Items in Task Group
        with TaskGroup("transform_order_items", tooltip="Data Transform Order Items") as transform_order_items:
            clean_order_items = CleanDataframeOperator(
                task_id=TASK_FETCH_ORDER_ITEMS,
                dataframe=data_ingestion.get_child_by_label(TASK_GROUP_FETCH_DATA_AVRO).get_child_by_label(TASK_FETCH_ORDER_ITEMS).output,
                drop_duplicates_columns=['id'])
            
            insert_order_items= InsertDataframeToPostgresOperator(
                task_id=TASK_INSERT_ORDER_ITEMS,
                dataframe=clean_order_items.output,
                table_name='order_items',
                conn_id=constants.POSTGRES_DW_CONN,
            )

            clean_order_items >> insert_order_items
        
        # Data Transform and Load for Orders in Task Group
        with TaskGroup("transform_orders", tooltip="Data Transform Orders") as transform_orders:
            clean_orders = CleanDataframeOperator(
                task_id=TASK_CLEAN_ORDERS,
                dataframe=data_ingestion.get_child_by_label(TASK_GROUP_FETCH_DATA_PARQUET).get_child_by_label(TASK_FETCH_ORDERS).output,
                drop_duplicates_columns=['id'],
                change_type_columns=[
                    ChangeType(column_name='created_at', new_type='datetime64[ns]'),
                ])
            
            insert_order_items= InsertDataframeToPostgresOperator(
                task_id=TASK_INSERT_ORDERS,
                dataframe=clean_orders.output,
                table_name='orders',
                conn_id=constants.POSTGRES_DW_CONN,
            )

            clean_orders >> insert_order_items
        
        # Data Transform and Load for Product Categories in Task Group
        with TaskGroup("transform_product_categories", tooltip="Data Transform Product Categories") as transform_product_categories:
            clean_product_categories = CleanDataframeOperator(
                task_id=TASK_CLEAN_PRODUCT_CATEGORIES,
                dataframe=data_ingestion.get_child_by_label(TASK_GROUP_FETCH_DATA_EXCEL).get_child_by_label(TASK_FETCH_PRODUCT_CATEGORIES).output,
                drop_columns=['Unnamed: 0'],
                drop_duplicates_columns=['id'])
            
            insert_product_categories= InsertDataframeToPostgresOperator(
                task_id=TASK_INSERT_PRODUCT_CATEGORIES,
                dataframe=clean_product_categories.output,
                table_name='product_categories',
                conn_id=constants.POSTGRES_DW_CONN,
            )

            clean_product_categories >> insert_product_categories

        # Data Transform and Load for Products in Task Group
        with TaskGroup("transform_products", tooltip="Data Transform Products") as transform_products:    
            clean_product = CleanDataframeOperator(
                task_id=TASK_CLEAN_PRODUCTS,
                dataframe=data_ingestion.get_child_by_label(TASK_GROUP_FETCH_DATA_EXCEL).get_child_by_label(TASK_FETCH_PRODUCTS).output,
                drop_columns=['Unnamed: 0'],
                drop_duplicates_columns=['id'],
                change_type_columns=[
                    ChangeType(column_name='name', new_type=str),
                    ChangeType(column_name='price', new_type=float),
                ])
            
            insert_products= InsertDataframeToPostgresOperator(
                task_id=TASK_INSERT_PRODUCTS,
                dataframe=clean_product.output,
                table_name='products',
                conn_id=constants.POSTGRES_DW_CONN,
            )
            clean_product >> insert_products
        
        # Data Transform and Load for Suppliers in Task Group
        with TaskGroup("transform_suppliers", tooltip="Data Transform Suppliers") as transform_suppliers:  
            clean_suppliers = CleanDataframeOperator(
                task_id=TASK_CLEAN_SUPPLIERS,
                dataframe=data_ingestion.get_child_by_label(TASK_GROUP_FETCH_DATA_EXCEL).get_child_by_label(TASK_FETCH_SUPPLIERS).output,
                drop_columns=['Unnamed: 0'],
                drop_duplicates_columns=['id'])
            
            insert_suppliers= InsertDataframeToPostgresOperator(
                task_id=TASK_INSERT_SUPPLIERS,
                dataframe=clean_suppliers.output,
                table_name='suppliers',
                conn_id=constants.POSTGRES_DW_CONN,
            )

            clean_suppliers >> insert_suppliers
        
        [transform_coupon, 
         transform_product_categories,
           transform_suppliers] >> transform_products >> transform_customers >> transform_orders >> transform_login_attemps >> transform_order_items

    data_ingestion >> data_transform

        