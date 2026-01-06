from airflow.models import DagBag

DAG_PATH = "dags"

def test_dag1_loaded():
    dagbag = DagBag(dag_folder=DAG_PATH, include_examples=False)
    assert "csv_to_postgres_ingestion" in dagbag.dags
    assert len(dagbag.import_errors) == 0

def test_dag1_structure():
    dagbag = DagBag(dag_folder=DAG_PATH, include_examples=False)
    dag = dagbag.get_dag("csv_to_postgres_ingestion")
    assert len(dag.tasks) == 3

def test_dag1_task_dependencies():
    dagbag = DagBag(dag_folder=DAG_PATH, include_examples=False)
    dag = dagbag.get_dag("csv_to_postgres_ingestion")

    create_task = dag.get_task("create_table_if_not_exists")
    truncate_task = dag.get_task("truncate_table")
    load_task = dag.get_task("load_csv_to_postgres")

    assert truncate_task.task_id in create_task.downstream_task_ids
    assert load_task.task_id in truncate_task.downstream_task_ids

def test_dag1_no_cycles():
    dagbag = DagBag(dag_folder=DAG_PATH, include_examples=False)
    dag = dagbag.get_dag("csv_to_postgres_ingestion")
    dag.test_cycle()

def test_dag1_schedule():
    dagbag = DagBag(dag_folder=DAG_PATH, include_examples=False)
    dag = dagbag.get_dag("csv_to_postgres_ingestion")
    assert dag.schedule_interval == "@daily"
