from airflow.models import DagBag
from unittest.mock import patch
from airflow.utils import timezone

DAG_ID = "ssis_error_handle_update_status"   # 你的 DAG 名
TASK_ID = "log_into_FILE_REC_HIS"

@patch("airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook.run")
def test_log_his_sql_and_params(mock_run):
    dag = DagBag().get_dag(DAG_ID)
    task = dag.get_task(TASK_ID)

    # 建 template context，模擬 dag_run.conf
    class DR: 
        run_id = "manual__2025-10-27T18:20:00+08:00"
        conf = {"file_name": "daily_sales_20251027.csv", "task_id_hint": "Load_File_Job_001"}
    ctx = {
        "ti": type("TI", (), {"task_id": "log_into_FILE_REC_HIS"})(),
        "dag_run": DR(),
        "execution_date": timezone.datetime(2025,1,1),
        "params": {},
    }

    task.execute(ctx)

    called = mock_run.call_args.kwargs
    assert "INSERT INTO dbo.FILE_REC_HIS" in called["sql"]
    assert called["parameters"]["source_id"] == "Load_File_Job_001"
    assert called["parameters"]["file_name"] == "daily_sales_20251027.csv"
    assert "manual__" in called["parameters"]["execution_id"]