from airflow.models import DagBag

DAG_PATH = "dags"

def test_all_dags_load():
    dagbag = DagBag(dag_folder=DAG_PATH, include_examples=False)

    expected_dags = {
        "csv_to_postgres_ingestion",
        "data_transformation_pipeline",
        "postgres_to_parquet_export",
        "conditional_workflow_pipeline",
        "notification_workflow",
    }

    assert expected_dags.issubset(set(dagbag.dags.keys()))
    assert len(dagbag.import_errors) == 0

def test_all_dag_ids_unique():
    dagbag = DagBag(dag_folder=DAG_PATH, include_examples=False)
    dag_ids = [dag.dag_id for dag in dagbag.dags.values()]
    assert len(dag_ids) == len(set(dag_ids))
