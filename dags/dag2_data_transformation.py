import dags.airflow_compat
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

def create_transformed_table():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    hook.run("""
        CREATE TABLE IF NOT EXISTS transformed_employee_data (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255),
            age INTEGER,
            city VARCHAR(100),
            salary FLOAT,
            join_date DATE,
            full_info VARCHAR(500),
            age_group VARCHAR(20),
            salary_category VARCHAR(20),
            year_joined INTEGER
        );
    """)

def transform_data():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()

    # Read raw data
    df = pd.read_sql("SELECT * FROM raw_employee_data", engine)
    rows_processed = int(df.shape[0])

    if rows_processed == 0:
        raise ValueError("No data found in raw_employee_data")

    # Transformations
    df["full_info"] = df["name"] + " - " + df["city"]

    df["age_group"] = df["age"].apply(
        lambda x: "Young" if x < 30 else "Mid" if x < 50 else "Senior"
    )

    df["salary_category"] = df["salary"].apply(
        lambda x: "Low" if x < 50000 else "Medium" if x < 80000 else "High"
    )

    df["year_joined"] = pd.to_datetime(df["join_date"]).dt.year

    # Load transformed data
    df.to_sql(
        "transformed_employee_data",
        engine,
        if_exists="replace",
        index=False
    )

    return {
        "rows_processed": rows_processed,
        "rows_inserted": int(df.shape[0])
    }

with DAG(
    dag_id="data_transformation_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["transformation"]
) as dag:

    create_table_task = PythonOperator(
        task_id="create_transformed_table",
        python_callable=create_transformed_table
    )

    transform_and_load_task = PythonOperator(
        task_id="transform_and_load",
        python_callable=transform_data
    )

    create_table_task >> transform_and_load_task
