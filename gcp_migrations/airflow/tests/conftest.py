import os
import sys
import pytest

# 專案根目錄
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../.."))
AIRFLOW_DIR = os.path.join(PROJECT_ROOT, "gcp_migrations", "airflow")

# 只加入「專案根」到 sys.path，避免把本地 airflow 套件頂掉官方 apache-airflow
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# 指定 AIRFLOW_HOME（只是路徑，不會造成匯入衝突）
os.environ.setdefault("AIRFLOW_HOME", AIRFLOW_DIR)

# 若沒裝 Airflow，整包跳過
try:
    import airflow  # noqa: F401
except ModuleNotFoundError:
    pytest.skip("Apache Airflow not installed; skipping Airflow tests.", allow_module_level=True)

# 只有需要 DagBag 時才匯入，且用正確路徑
@pytest.fixture(scope="session")
def dag_bag():
    from airflow.models.dagbag import DagBag
    return DagBag(dag_folder=os.path.join(AIRFLOW_DIR, "dags"), include_examples=False)
