from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os

# Оптимизированная универсальная функция для ускоренной заливки CSV в слой RAW
def load_csv_to_raw(file_name, table_name):
    csv_path = f'/opt/airflow/data/{file_name}'
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Файл {csv_path} не найден в папке data!")
        
    print(f"Начало импорта {file_name}...")
    
    # Читаем всё как string, чтобы избежать ошибок несовпадения типов на сыром этапе
    df = pd.read_csv(csv_path, dtype=str)
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_analytic')
    engine = pg_hook.get_sqlalchemy_engine()
    
    # Скидываем в схему raw пачками по 10 000 строк (ускоряет инсерт в десятки раз!)
    df.to_sql(
        table_name, 
        schema='raw', 
        con=engine, 
        if_exists='replace', 
        index=False,
        chunksize=10000,
        method='multi'
    )
    print(f"Успешно загружено в raw.{table_name} ({len(df)} строк)")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
}

with DAG(
    dag_id='olist_full_elt_pipeline',
    default_args=default_args,
    schedule_interval=None,  
    catchup=False,
    tags=['elt', 'olist', 'dwh']
) as dag:

    # 1. СОЗДАНИЕ СТРУКТУР (Индексы, схемы, партиции)
    prepare_db_structures = PostgresOperator(
        task_id='prepare_db_structures',
        postgres_conn_id='postgres_analytic',
        sql="""
        CREATE SCHEMA IF NOT EXISTS raw;
        CREATE SCHEMA IF NOT EXISTS staging;

        -- ==========================================
        -- 1. ТАБЛИЦА ЗАКАЗОВ (ПАРТИЦИОНИРОВАННАЯ)
        -- ==========================================
        CREATE TABLE IF NOT EXISTS staging.orders (
            order_id VARCHAR(50),
            customer_id VARCHAR(50),
            order_status VARCHAR(30),
            order_purchase_timestamp TIMESTAMP,
            order_approved_at TIMESTAMP,
            order_delivered_carrier_date TIMESTAMP,
            order_delivered_customer_date TIMESTAMP,
            order_estimated_delivery_date TIMESTAMP,
            PRIMARY KEY (order_id, order_purchase_timestamp)
        ) PARTITION BY RANGE (order_purchase_timestamp);

        -- Явные партиции под временные рамки Olist (2016 - 2018 гг.)
        CREATE TABLE IF NOT EXISTS staging.orders_2016 PARTITION OF staging.orders FOR VALUES FROM ('2016-01-01') TO ('2017-01-01');
        CREATE TABLE IF NOT EXISTS staging.orders_2017 PARTITION OF staging.orders FOR VALUES FROM ('2017-01-01') TO ('2018-01-01');
        CREATE TABLE IF NOT EXISTS staging.orders_2018 PARTITION OF staging.orders FOR VALUES FROM ('2018-01-01') TO ('2019-01-01');
        CREATE TABLE IF NOT EXISTS staging.orders_default PARTITION OF staging.orders DEFAULT;

        -- ==========================================
        -- 2. ОСТАЛЬНЫЕ ТАБЛИЦЫ СВЯЗЕЙ (STAGING)
        -- ==========================================
        CREATE TABLE IF NOT EXISTS staging.customers (
            customer_id VARCHAR(50) PRIMARY KEY,
            customer_unique_id VARCHAR(50),
            customer_zip_code_prefix VARCHAR(20),
            customer_city VARCHAR(100),
            customer_state VARCHAR(10)
        );

        CREATE TABLE IF NOT EXISTS staging.order_items (
            order_id VARCHAR(50),
            order_item_id INT,
            product_id VARCHAR(50),
            seller_id VARCHAR(50),
            shipping_limit_date TIMESTAMP,
            price NUMERIC(10, 2),
            freight_value NUMERIC(10, 2),
            PRIMARY KEY (order_id, order_item_id)
        );

        CREATE TABLE IF NOT EXISTS staging.order_payments (
            order_id VARCHAR(50),
            payment_sequential INT,
            payment_type VARCHAR(30),
            payment_installments INT,
            payment_value NUMERIC(10, 2),
            PRIMARY KEY (order_id, payment_sequential)
        );

        CREATE TABLE IF NOT EXISTS staging.order_reviews (
            review_id VARCHAR(50) PRIMARY KEY,
            order_id VARCHAR(50),
            review_score INT,
            review_comment_title TEXT,
            review_comment_message TEXT,
            review_creation_date TIMESTAMP,
            review_answer_timestamp TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS staging.products (
            product_id VARCHAR(50) PRIMARY KEY,
            product_category_name VARCHAR(100),
            product_name_lenght INT,
            product_description_lenght INT,
            product_photos_qty INT,
            product_weight_g INT,
            product_length_cm INT,
            product_height_cm INT,
            product_width_cm INT
        );

        CREATE TABLE IF NOT EXISTS staging.sellers (
            seller_id VARCHAR(50) PRIMARY KEY,
            seller_zip_code_prefix VARCHAR(20),
            seller_city VARCHAR(100),
            seller_state VARCHAR(10)
        );

        CREATE TABLE IF NOT EXISTS staging.geolocation (
            geolocation_zip_code_prefix VARCHAR(20),
            geolocation_lat NUMERIC(12, 9),
            geolocation_lng NUMERIC(12, 9),
            geolocation_city VARCHAR(100),
            geolocation_state VARCHAR(10)
        );

        CREATE TABLE IF NOT EXISTS staging.product_category_translation (
            product_category_name VARCHAR(100) PRIMARY KEY,
            product_category_name_english VARCHAR(100)
        );

        -- ==========================================
        -- 3. СОЗДАНИЕ ИНДЕКСОВ ДЛЯ ОПТИМИЗАЦИИ
        -- ==========================================
        CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON staging.orders(customer_id);
        CREATE INDEX IF NOT EXISTS idx_items_product_id ON staging.order_items(product_id);
        CREATE INDEX IF NOT EXISTS idx_items_seller_id ON staging.order_items(seller_id);
        CREATE INDEX IF NOT EXISTS idx_payments_order_id ON staging.order_payments(order_id);
        """
    )

    # 2. ИМПОРТ ИЗ CSV В RAW СЛОЙ (Параллельные таски)
    csv_files = {
        'customers': ('olist_customers_dataset.csv', 'customers_raw'),
        'geolocation': ('olist_geolocation_dataset.csv', 'geolocation_raw'),
        'order_items': ('olist_order_items_dataset.csv', 'order_items_raw'),
        'order_payments': ('olist_order_payments_dataset.csv', 'order_payments_raw'),
        'order_reviews': ('olist_order_reviews_dataset.csv', 'order_reviews_raw'),
        'orders': ('olist_orders_dataset.csv', 'orders_raw'),
        'products': ('olist_products_dataset.csv', 'products_raw'),
        'sellers': ('olist_sellers_dataset.csv', 'sellers_raw'),
        'translation': ('product_category_name_translation.csv', 'category_translation_raw'),
    }

    extract_load_tasks = {}
    for key, (file_name, table_name) in csv_files.items():
        extract_load_tasks[key] = PythonOperator(
            task_id=f'load_csv_{key}',
            python_callable=load_csv_to_raw,
            op_kwargs={'file_name': file_name, 'table_name': table_name}
        )

    # 3. ТРАНСФОРМАЦИЯ И ПЕРЕНОС В СЛОЙ STAGING (Кастинг типов)
    transform_and_stage = PostgresOperator(
        task_id='transform_and_stage',
        postgres_conn_id='postgres_analytic',
        sql="""
        -- Очищаем таблицы перед инсертом (для идемпотентности)
        TRUNCATE TABLE staging.customers CASCADE;
        TRUNCATE TABLE staging.geolocation CASCADE;
        TRUNCATE TABLE staging.order_items CASCADE;
        TRUNCATE TABLE staging.order_payments CASCADE;
        TRUNCATE TABLE staging.order_reviews CASCADE;
        TRUNCATE TABLE staging.orders CASCADE;
        TRUNCATE TABLE staging.products CASCADE;
        TRUNCATE TABLE staging.sellers CASCADE;
        TRUNCATE TABLE staging.product_category_translation CASCADE;

        -- 1. Сustomers
        INSERT INTO staging.customers SELECT customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state FROM raw.customers_raw;

        -- 2. Sellers
        INSERT INTO staging.sellers SELECT seller_id, seller_zip_code_prefix, seller_city, seller_state FROM raw.sellers_raw;

        -- 3. Geolocation (числа с плавающей точкой)
        INSERT INTO staging.geolocation 
        SELECT geolocation_zip_code_prefix, NULLIF(geolocation_lat, '')::NUMERIC, NULLIF(geolocation_lng, '')::NUMERIC, geolocation_city, geolocation_state FROM raw.geolocation_raw;

        -- 4. Products (каст пустых строк в NULL и затем в INT)
        INSERT INTO staging.products 
        SELECT product_id, product_category_name, NULLIF(product_name_lenght, '')::INT, NULLIF(product_description_lenght, '')::INT, 
               NULLIF(product_photos_qty, '')::INT, NULLIF(product_weight_g, '')::INT, NULLIF(product_length_cm, '')::INT, 
               NULLIF(product_height_cm, '')::INT, NULLIF(product_width_cm, '')::INT 
        FROM raw.products_raw;

        -- 5. Category Translation
        INSERT INTO staging.product_category_translation SELECT product_category_name, product_category_name_english FROM raw.category_translation_raw;

        -- 6. Orders (каст строк в TIMESTAMP)
        INSERT INTO staging.orders 
        SELECT order_id, customer_id, order_status, 
               NULLIF(order_purchase_timestamp, '')::TIMESTAMP, NULLIF(order_approved_at, '')::TIMESTAMP, 
               NULLIF(order_delivered_carrier_date, '')::TIMESTAMP, NULLIF(order_delivered_customer_date, '')::TIMESTAMP, 
               NULLIF(order_estimated_delivery_date, '')::TIMESTAMP 
        FROM raw.orders_raw;

        -- 7. Order Items (каст дат и денежных типов NUMERIC)
        INSERT INTO staging.order_items 
        SELECT order_id, order_item_id::INT, product_id, seller_id, NULLIF(shipping_limit_date, '')::TIMESTAMP, price::NUMERIC, freight_value::NUMERIC 
        FROM raw.order_items_raw;

        -- 8. Order Payments
        INSERT INTO staging.order_payments 
        SELECT order_id, payment_sequential::INT, payment_type, payment_installments::INT, payment_value::NUMERIC 
        FROM raw.order_payments_raw;

        -- 9. Order Reviews
        INSERT INTO staging.order_reviews 
        SELECT DISTINCT ON (review_id)
               review_id, 
               order_id, 
               review_score::INT, 
               review_comment_title, 
               review_comment_message, 
               NULLIF(review_creation_date, '')::TIMESTAMP, 
               NULLIF(review_answer_timestamp, '')::TIMESTAMP 
        FROM raw.order_reviews_raw
        ORDER BY review_id; 
        """
    )

    # Задаем архитектурную схему зависимостей
    prepare_db_structures >> list(extract_load_tasks.values()) >> transform_and_stage