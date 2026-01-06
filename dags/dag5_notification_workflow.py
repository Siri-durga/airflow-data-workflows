from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

def send_success_notification(context):
    return {
        "notification_type": "success",
        "status": "sent",
        "message": f"Task {context['task_instance'].task_id} succeeded",
        "timestamp": str(context["execution_date"])
    }

def send_failure_notification(context):
    return {
        "notification_type": "failure",
        "status": "sent",
        "message": f"Task {context['task_instance'].task_id} failed",
        "error": str(context.get("exception")),
        "timestamp": str(context["execution_date"])
    }

def risky_operation(**context):
    execution_date = context["execution_date"]
    day_of_month = execution_date.day

    if day_of_month % 5 == 0:
        raise Exception("Simulated failure: day divisible by 5")

    return {
        "status": "success",
        "execution_date": str(execution_date),
        "success": True
    }

def cleanup_task(**context):
    return {
        "cleanup_status": "completed",
        "timestamp": str(context["execution_date"])
    }

with DAG(
    dag_id="notification_workflow",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["notifications"]
) as dag:

    start_task = EmptyOperator(task_id="start_task")

    risky_task = PythonOperator(
        task_id="risky_operation",
        python_callable=risky_operation,
        on_success_callback=send_success_notification,
        on_failure_callback=send_failure_notification
    )

    success_notification_task = EmptyOperator(
        task_id="success_notification",
        trigger_rule="all_success"
    )

    failure_notification_task = EmptyOperator(
        task_id="failure_notification",
        trigger_rule="all_failed"
    )

    always_execute_task = PythonOperator(
        task_id="always_execute",
        python_callable=cleanup_task,
        trigger_rule="all_done"
    )

    start_task >> risky_task
    risky_task >> [success_notification_task, failure_notification_task]
    [success_notification_task, failure_notification_task] >> always_execute_task
