import os
from airflow.models import DagBag

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DAG_PATH = os.path.join(BASE_DIR, "dags")

def get_dagbag():
    return DagBag(
        dag_folder=DAG_PATH,
        include_examples=False,
        read_dags_from_db=False
    )

def test_dag2_loaded():
    dagbag = get_dagbag()
    assert "data_transformation_pipeline" in dagbag.dags

def test_dag2_structure():
    dag = get_dagbag().get_dag("data_transformation_pipeline")
    assert len(dag.tasks) == 2

def test_dag2_dependencies():
    dag = get_dagbag().get_dag("data_transformation_pipeline")

    create_task = dag.get_task("create_transformed_table")
    transform_task = dag.get_task("transform_and_load")

    assert transform_task.task_id in create_task.downstream_task_ids

def test_dag2_no_cycles():
    dag = get_dagbag().get_dag("data_transformation_pipeline")
    dag.test_cycle()

def test_dag2_schedule():
    dag = get_dagbag().get_dag("data_transformation_pipeline")
    assert dag.schedule_interval == "@daily"
