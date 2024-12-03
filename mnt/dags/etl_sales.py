from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import pandas as pd
import boto3
from io import StringIO
from sqlalchemy import create_engine
import psycopg2

# Default arguments for the DAG
default_args = {
    'owner': 'k.norasit',
    'depends_on_past': False,
    'retries': 0,
}

# กำหนด Connection URLs
postgres_url = "postgresql+psycopg2://postgres_user:postgres_password@postgres:5432/my_db"
engine = create_engine(postgres_url)

# ฟังก์ชันสำหรับโหลดข้อมูล user_info
def load_user_info_data_to_postgres():

    # สร้าง MinIO Client
    minio_client = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='admin',
        aws_secret_access_key='password'
    )

    bucket_name = "rawdata"
    file_key = "user_info.csv"
    table_name = "user_info"

    # ดึงไฟล์จาก MinIO
    obj = minio_client.get_object(Bucket=bucket_name, Key=file_key)
    df = pd.read_csv(obj['Body'])

    # เชื่อมต่อ PostgreSQL
    conn = psycopg2.connect(
        dbname="my_db",
        user="postgres_user",
        password="postgres_password",
        host="postgres",
        port="5432"
    )
    cur = conn.cursor()

    # Upsert ข้อมูลทีละแถว
    for _, row in df.iterrows():
        upsert_query = """
        INSERT INTO user_info (user_id, age, gender, location)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (user_id)
        DO UPDATE SET
            age = EXCLUDED.age,
            gender = EXCLUDED.gender,
            location = EXCLUDED.location;
        """
        cur.execute(upsert_query, (row['user_id'], row['age'], row['gender'], row['location']))

    # Commit และปิดการเชื่อมต่อ
    conn.commit()
    cur.close()
    conn.close()

def load_transaction_data_to_postgres():
    # สร้าง MinIO Client
    minio_client = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='admin',
        aws_secret_access_key='password'
    )

    bucket_name = "rawdata"
    prefix = "sales"
    table_name = "transaction"

    # เชื่อมต่อ PostgreSQL
    conn = psycopg2.connect(
        dbname="my_db",
        user="postgres_user",
        password="postgres_password",
        host="postgres",
        port="5432"
    )
    cur = conn.cursor()

    # ดึงรายชื่อไฟล์จาก MinIO
    response = minio_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if "Contents" not in response:
        print("No transaction files found.")
        return

    # ดึงรายชื่อไฟล์ที่เคยโหลดแล้ว
    cur.execute("SELECT file_name FROM loaded_files")
    loaded_files = {row[0] for row in cur.fetchall()}

    # โหลดเฉพาะไฟล์ใหม่
    for obj in response["Contents"]:
        file_key = obj["Key"]
        if file_key in loaded_files:
            print(f"Skipping already loaded file: {file_key}")
            continue

        print(f"Processing new file: {file_key}")
        
        # ดึงไฟล์จาก MinIO
        obj = minio_client.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_csv(obj['Body'])

        # เขียนข้อมูลลง PostgreSQL
        df.to_sql(table_name, con=engine, if_exists='append', index=False)

        # บันทึกชื่อไฟล์ลง Metadata Table
        cur.execute(
            "INSERT INTO loaded_files (file_name) VALUES (%s)",
            (file_key,)
        )

    # Commit และปิดการเชื่อมต่อ
    conn.commit()
    cur.close()
    conn.close()

# Define the DAG
with DAG(
    dag_id='etl_process_dag',
    default_args=default_args,
    description='ETL Process DAG for MinIO and PostgreSQL',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task 2-8: Create tables and truncate where needed
    create_user_info_table = PostgresOperator(
        task_id='create_user_info_table',
        postgres_conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS user_info (
            user_id INT PRIMARY KEY,
            age INT,
            gender CHAR(1),
            location VARCHAR(50)
        );
        """
    )

    create_transaction_table = PostgresOperator(
        task_id='create_transaction_table',
        postgres_conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS transaction (
            order_id INT PRIMARY KEY,
            user_id INT,
            product_id INT,
            quantity INT,
            amount DECIMAL(10, 2),
            order_date DATE
        );
        """
    )

    create_user_transaction_amount_table = PostgresOperator(
        task_id='create_user_transaction_amount_table',
        postgres_conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS user_transaction_amount (
            user_id INT PRIMARY KEY,
            total_sales NUMERIC,
            average_sales NUMERIC
        );
        TRUNCATE TABLE user_transaction_amount;
        """
    )

    create_daily_transaction_amount_table = PostgresOperator(
        task_id='create_daily_transaction_amount_table',
        postgres_conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS daily_transaction_amount (
            order_date DATE PRIMARY KEY,
            total_sales NUMERIC,
            min_sales NUMERIC,
            max_sales NUMERIC,
            average_sales NUMERIC,
            vat NUMERIC
        );
        TRUNCATE TABLE daily_transaction_amount;
        """
    )

    create_product_sales_table = PostgresOperator(
        task_id='create_product_sales_table',
        postgres_conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS product_sales (
            product_id INT PRIMARY KEY,
            total_quantity INT,
            total_sales NUMERIC
        );
        TRUNCATE TABLE product_sales;
        """
    )

    create_user_location_sales_table = PostgresOperator(
        task_id='create_user_location_sales_table',
        postgres_conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS user_location_sales (
            location VARCHAR(50),
            gender CHAR(1),
            total_sales NUMERIC,
            min_sales NUMERIC,
            max_sales NUMERIC,
            average_sales NUMERIC
        );
        TRUNCATE TABLE user_location_sales;
        """
    )

    create_top_products_table = PostgresOperator(
        task_id='create_top_products_table',
        postgres_conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS top_products_in_Chiangmai (
            order_date DATE,
            rank_num INT,
            product_id INT,
            total_sales NUMERIC,
            total_quantity INT
        );
        TRUNCATE TABLE top_products_in_Chiangmai;
        """
    )

    create_loaded_files_table = PostgresOperator(
        task_id='create_loaded_files_table',
        postgres_conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS loaded_files (
            file_name TEXT PRIMARY KEY,
            load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        )

    # Empty operator to separate groups
    empty1 = EmptyOperator(task_id='empty1')
    empty2 = EmptyOperator(task_id='empty2')

    # Task 9: Load user_info.csv into PostgreSQL
    load_user_info_data = PythonOperator(
        task_id='load_user_info_data',
        python_callable=load_user_info_data_to_postgres,
        dag=dag,
    )

    # Task 10: Load all sales*.csv files into PostgreSQL
    load_transaction_data = PythonOperator(
        task_id='load_transaction_data',
        python_callable=load_transaction_data_to_postgres,
        dag=dag,
    )

    # Task 11-15: Perform aggregations
    calculate_user_transaction_amount = PostgresOperator(
        task_id='calculate_user_transaction_amount',
        postgres_conn_id='postgres_default',
        sql="""
        INSERT INTO user_transaction_amount (user_id, total_sales, average_sales)
        SELECT user_id, SUM(quantity * amount), AVG(quantity * amount)
        FROM transaction
        GROUP BY user_id;
        """
    )

    calculate_daily_transaction_amount = PostgresOperator(
        task_id='calculate_daily_transaction_amount',
        postgres_conn_id='postgres_default',
        sql="""
        INSERT INTO daily_transaction_amount (order_date, total_sales, min_sales, max_sales, average_sales, vat)
        SELECT order_date, SUM(quantity * amount), MIN(quantity * amount), MAX(quantity * amount),
               AVG(quantity * amount), SUM(quantity * amount) * 0.07
        FROM transaction
        GROUP BY order_date;
        """
    )

    calculate_product_sales = PostgresOperator(
        task_id='calculate_product_sales',
        postgres_conn_id='postgres_default',
        sql="""
        INSERT INTO product_sales (product_id, total_quantity, total_sales)
        SELECT product_id, SUM(quantity), SUM(quantity * amount)
        FROM transaction
        GROUP BY product_id;
        """
    )

    calculate_user_location_sales = PostgresOperator(
        task_id='calculate_user_location_sales',
        postgres_conn_id='postgres_default',
        sql="""
        INSERT INTO user_location_sales (location, gender, total_sales, min_sales, max_sales, average_sales)
        SELECT ui.location, ui.gender, SUM(t.quantity * t.amount),
               MIN(t.quantity * t.amount), MAX(t.quantity * t.amount), AVG(t.quantity * t.amount)
        FROM transaction t
        JOIN user_info ui ON t.user_id = ui.user_id
        GROUP BY ui.location, ui.gender;
        """
    )

    calculate_top_products = PostgresOperator(
        task_id='calculate_top_products',
        postgres_conn_id='postgres_default',
        sql="""
        INSERT INTO top_products_in_Chiangmai (order_date, rank_num, product_id, total_sales, total_quantity)
        SELECT *
        FROM (
            SELECT 
                order_date, 
                RANK() OVER (PARTITION BY order_date ORDER BY SUM(quantity * amount) DESC) AS rank_num,
                product_id, 
                SUM(quantity * amount) AS total_sales, 
                SUM(quantity) AS total_quantity
            FROM transaction t
            JOIN user_info ui ON t.user_id = ui.user_id
            WHERE ui.location = 'Chiangmai'
            GROUP BY order_date, product_id
        ) ranked_products
        WHERE rank_num <= 20;
        """
    )

    # Define the task dependencies
    [
        create_user_info_table,
        create_transaction_table,
        create_user_transaction_amount_table,
        create_daily_transaction_amount_table,
        create_product_sales_table,
        create_user_location_sales_table,
        create_top_products_table,
        create_loaded_files_table
    ] >> empty1 >> [load_user_info_data, load_transaction_data] >> empty2 >> [
        calculate_user_transaction_amount,
        calculate_daily_transaction_amount,
        calculate_product_sales,
        calculate_user_location_sales,
        calculate_top_products,
    ]
