import os, sys, importlib.util, types, pytest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../.."))
DAGS_DIR = os.path.join(PROJECT_ROOT, "gcp_migrations", "airflow", "dags")
AIRFLOW_DIR = os.path.join(PROJECT_ROOT, "gcp_migrations", "airflow")

# 指定 AIRFLOW_HOME（只是路徑，不會造成匯入衝突）
os.environ.setdefault("AIRFLOW_HOME", AIRFLOW_DIR)

# 只加入「專案根」到 sys.path，避免把本地 airflow 套件頂掉官方 apache-airflow
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# 若沒裝 Airflow，整包跳過
try:
    import airflow  # noqa: F401
except ModuleNotFoundError:
    pytest.skip("Apache Airflow not installed; skipping Airflow tests.", allow_module_level=True)


def _load_module_from_file(py_path: str) -> types.ModuleType:
    spec = importlib.util.spec_from_file_location("dynamic_loaded", py_path)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod

@pytest.fixture(scope="session")
def dag_module():
    """動態載入單一 DAG 檔案，不要求 dags/ 成為 Python 套件。"""
    path = os.path.join(DAGS_DIR, "retail_etl_daily.py")
    return _load_module_from_file(path)

@pytest.fixture(scope="session")
def dag(dag_module):
    """抓出模組中的 DAG 物件；若有多個，挑主要那個。"""
    from airflow.models import DAG
    for v in dag_module.__dict__.values():
        if isinstance(v, DAG):
            return v
    raise AssertionError("No DAG instance found in retail_etl_daily.py")

# 只有需要 DagBag 時才匯入，且用正確路徑
@pytest.fixture(scope="session")
def dag_bag():
    from airflow.models.dagbag import DagBag
    return DagBag(dag_folder=os.path.join(AIRFLOW_DIR, "dags"), include_examples=False)
