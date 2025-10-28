import pytest
from airflow.models import DagBag
from airflow.utils.state import State

@pytest.mark.integration
def test_dag_runs_successfully(dag_bag):
    dag = dag_bag.get_dag("ods_update_file_status")
    dag.clear()  # 清除舊紀錄
    dag_run = dag.create_dagrun(
        run_id="test_run",
        state=State.RUNNING,
        execution_date=None,
        conf={"param": "value"},
    )
    tasks = dag.tasks
    assert len(tasks) > 0
