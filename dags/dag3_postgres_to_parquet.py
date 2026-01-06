from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os

def check_table_exists(table_name: str):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    records = hook.get_records(
        f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
    )

    if records[0][0] == 0:
        raise ValueError(f"Table {table_name} does not exist")

    count = hook.get_first(f"SELECT COUNT(*) FROM {table_name}")[0]
    if count == 0:
        raise ValueError(f"Table {table_name} exists but contains no data")

    return True

def export_table_to_parquet(table_name: str, output_path: str):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()

    df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
    row_count = int(df.shape[0])

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df.to_parquet(
        output_path,
        engine="pyarrow",
        compression="snappy"
    )

    file_size = os.path.getsize(output_path)

    return {
        "file_path": output_path,
        "row_count": row_count,
        "file_size_bytes": file_size
    }

def validate_parquet(file_path: str):
    if not os.path.exists(file_path):
        raise FileNotFoundError("Parquet file not found")

    df = pd.read_parquet(file_path)

    required_columns = {
        "id", "name", "age", "city", "salary", "join_date",
        "full_info", "age_group", "salary_category", "year_joined"
    }

    if not required_columns.issubset(set(df.columns)):
        raise ValueError("Parquet schema validation failed")

    if df.shape[0] == 0:
        raise ValueError("Parquet file contains no data")

    return True

with DAG(
    dag_id="postgres_to_parquet_export",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
    tags=["export", "parquet"]
) as dag:

    check_table_task = PythonOperator(
        task_id="check_source_table_exists",
        python_callable=check_table_exists,
        op_kwargs={"table_name": "transformed_employee_data"}
    )

    export_task = PythonOperator(
        task_id="export_to_parquet",
        python_callable=export_table_to_parquet,
        op_kwargs={
            "table_name": "transformed_employee_data",
            "output_path": "/opt/airflow/output/employee_data_{{ ds }}.parquet"
        }
    )

    validate_task = PythonOperator(
        task_id="validate_parquet_file",
        python_callable=validate_parquet,
        op_kwargs={
            "file_path": "/opt/airflow/output/employee_data_{{ ds }}.parquet"
        }
    )

    check_table_task >> export_task >> validate_task
