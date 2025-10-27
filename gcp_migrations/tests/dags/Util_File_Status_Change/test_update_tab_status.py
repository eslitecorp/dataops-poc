from airflow.models import DagBag
from unittest.mock import patch
from airflow.utils import timezone

DAG_ID = "ssis_error_handle_update_status"      # 你的 DAG id
TASK_ID = "update_TAB_STATUS_as_error"

@patch("airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook.run")
def test_sql_and_param(mock_run):
    dag = DagBag().get_dag(DAG_ID)
    task = dag.get_task(TASK_ID)

    class DR:
        run_id = "manual__2025-10-27T18:21:00+08:00"
        conf = {"file_name": "daily_sales_20251027.csv"}

    ctx = {
        "execution_date": timezone.datetime(2025,1,1),
        "dag_run": DR(),
        "ti": type("TI", (), {"task_id": TASK_ID})(),
        "params": {},
    }

    task.execute(ctx)

    called = mock_run.call_args.kwargs
    assert "UPDATE dbo.STAGE_TAB_ACT" in called["sql"]
    assert called["parameters"]["file_name"] == "daily_sales_20251027.csv"
