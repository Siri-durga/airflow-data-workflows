from airflow.models.dag import DAG
from airflow.utils.dag_cycle_tester import check_cycle

def test_cycle(self):
    """
    Compatibility method for older test suites.
    Raises exception if cycle exists.
    """
    check_cycle(self)

# Monkey-patch DAG
DAG.test_cycle = test_cycle
