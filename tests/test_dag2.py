from airflow.models import DagBag

DAG_PATH = "dags"

def test_dag2_loaded():
    dagbag = DagBag(dag_folder=DAG_PATH, include_examples=False)
    assert "data_transformation_pipeline" in dagbag.dags
    assert len(dagbag.import_errors) == 0

def test_dag2_structure():
    dagbag = DagBag(dag_folder=DAG_PATH, include_examples=False)
    dag = dagbag.get_dag("data_transformation_pipeline")
    assert len(dag.tasks) == 2

def test_dag2_dependencies():
    dagbag = DagBag(dag_folder=DAG_PATH, include_examples=False)
    dag = dagbag.get_dag("data_transformation_pipeline")

    create_task = dag.get_task("create_transformed_table")
    transform_task = dag.get_task("transform_and_load")

    assert transform_task.task_id in create_task.downstream_task_ids

def test_dag2_no_cycles():
    dagbag = DagBag(dag_folder=DAG_PATH, include_examples=False)
    dag = dagbag.get_dag("data_transformation_pipeline")
    dag.test_cycle()

def test_dag2_schedule():
    dagbag = DagBag(dag_folder=DAG_PATH, include_examples=False)
    dag = dagbag.get_dag("data_transformation_pipeline")
    assert dag.schedule_interval == "@daily"
