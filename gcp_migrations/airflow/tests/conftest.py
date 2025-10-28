import os
import pytest
from airflow.models import DagBag

@pytest.fixture(scope="session")
def dag_bag():
    """Load all DAGs once for DAG integrity tests."""
    os.environ["AIRFLOW_HOME"] = os.path.abspath("gcp_migrations/airflow")
    return DagBag(dag_folder=os.path.join(os.environ["AIRFLOW_HOME"], "dags"))

@pytest.fixture
def sample_dag(dag_bag):
    """Return a specific DAG by ID."""
    return dag_bag.get_dag("example_dag_id")  # 修改成你的DAG ID
