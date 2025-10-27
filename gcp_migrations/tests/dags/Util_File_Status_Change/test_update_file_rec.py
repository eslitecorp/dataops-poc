import pytest
from airflow.models import DagBag
from airflow.utils import timezone
from unittest.mock import patch

# 調整成你的實際 DAG / Task 名稱
DAG_ID = "ssis_error_handle_update_status"
TASK_ID = "update_FILE_REC"

def _build_ctx_with_conf():
    """建立帶 dag_run.conf 的 template context"""
    class DR:
        run_id = "manual__2025-10-27T18:00:00+08:00"
        conf = {
            "file_status": 2,
            "file_name": "daily_sales_20251027.csv",
            "task_id_hint": "Load_File_Job_001",
        }
    # ti 只需要 task_id 供模板使用
    TI = type("TI", (), {"task_id": TASK_ID})
    return {
        "execution_date": timezone.datetime(2025, 1, 1),
        "dag_run": DR(),
        "ti": TI(),
        "params": {},  # 無需 params
    }

def _build_ctx_with_params():
    """建立只用 params（無 dag_run.conf）的 template context，方便 tasks test 場景"""
    class DR:
        run_id = "manual__2025-10-27T18:05:00+08:00"
        conf = {}  # 故意留空，走 params 後備
    TI = type("TI", (), {"task_id": TASK_ID})
    return {
        "execution_date": timezone.datetime(2025, 1, 1),
        "dag_run": DR(),
        "ti": TI(),
        "params": {
            "file_status": 1,
            "file_name": "test.csv",
        },
    }

def test_dag_loaded():
    dagbag = DagBag()
    assert DAG_ID in dagbag.dags, f"{DAG_ID} not found"
    assert not dagbag.import_errors

@patch("airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook.run")
def test_update_file_rec_with_conf(mock_run):
    """驗證：從 dag_run.conf 取值，具名參數是否正確傳入 MsSqlHook.run"""
    dag = DagBag().get_dag(DAG_ID)
    task = dag.get_task(TASK_ID)

    ctx = _build_ctx_with_conf()
    task.execute(ctx)

    called = mock_run.call_args.kwargs
    assert "UPDATE dbo.FILE_REC" in called["sql"]

    params = called["parameters"]
    # 依我們的 MsSqlOperator(parameters={...}) 設計，應有這四個 key
    assert params["file_status"] == 2
    assert params["file_name"] == "daily_sales_20251027.csv"
    assert params["source_id"] == "Load_File_Job_001"     # 由 dag_run.conf.task_id_hint
    assert "manual__" in params["execution_id"]           # 由 dag_run.run_id

@patch("airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook.run")
def test_update_file_rec_with_params_fallback(mock_run):
    """驗證：沒有 dag_run.conf 時，會 fallback 到 params"""
    dag = DagBag().get_dag(DAG_ID)
    task = dag.get_task(TASK_ID)

    ctx = _build_ctx_with_params()
    task.execute(ctx)

    called = mock_run.call_args.kwargs
    params = called["parameters"]
    assert params["file_status"] == 1
    assert params["file_name"] == "test.csv"
    # fallback 時 source_id 會取 ti.task_id（若你的實作如此）
    assert params["source_id"] == TASK_ID
    assert "manual__" in params["execution_id"]