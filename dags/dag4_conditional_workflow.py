from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

def determine_branch(**context):
    execution_date = context["execution_date"]
    day_of_week = execution_date.weekday()  # Monday=0, Sunday=6

    if day_of_week <= 2:
        return "weekday_processing"
    elif day_of_week <= 4:
        return "end_of_week_processing"
    else:
        return "weekend_processing"

def weekday_process(**context):
    return {
        "day_name": context["execution_date"].strftime("%A"),
        "task_type": "weekday",
        "record_count": 100
    }

def end_of_week_process(**context):
    return {
        "day_name": context["execution_date"].strftime("%A"),
        "task_type": "end_of_week",
        "weekly_summary": "Weekly aggregation completed"
    }

def weekend_process(**context):
    return {
        "day_name": context["execution_date"].strftime("%A"),
        "task_type": "weekend",
        "cleanup_status": "Cleanup executed"
    }

with DAG(
    dag_id="conditional_workflow_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["branching"]
) as dag:

    start_task = EmptyOperator(task_id="start")

    branch_task = BranchPythonOperator(
        task_id="branch_by_day",
        python_callable=determine_branch
    )

    weekday_task = PythonOperator(
        task_id="weekday_processing",
        python_callable=weekday_process
    )

    weekday_summary = EmptyOperator(task_id="weekday_summary")

    end_of_week_task = PythonOperator(
        task_id="end_of_week_processing",
        python_callable=end_of_week_process
    )

    end_of_week_report = EmptyOperator(task_id="end_of_week_report")

    weekend_task = PythonOperator(
        task_id="weekend_processing",
        python_callable=weekend_process
    )

    weekend_cleanup = EmptyOperator(task_id="weekend_cleanup")

    end_task = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success"
    )

    start_task >> branch_task

    branch_task >> weekday_task >> weekday_summary >> end_task
    branch_task >> end_of_week_task >> end_of_week_report >> end_task
    branch_task >> weekend_task >> weekend_cleanup >> end_task
