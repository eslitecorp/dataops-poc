from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.python import PythonSensor

DAG_ID = "wait_for_file_and_unlock"

FILE_PATH = "/mnt/share/incoming/example.csv"   # 對應 SSIS 的 fileConnectionString
POKE_SECS = 2

def file_is_unlocked(ti, path: str, stable_secs: int = 4) -> bool:
    """
    回傳 True 表示：檔案存在、可開啟，且在 stable_secs 期間內 size/mtime 都未變動。
    （等同「不再被其他程序寫入中」的近似條件）
    """
    import os, time

    if not os.path.exists(path):
        return False

    # 先嘗試開啟（Windows 若被鎖住會丟 IOError；Linux 仍可開，但我們還會做「穩定」檢查）
    try:
        with open(path, "rb") as _f:
            pass
    except OSError:
        return False

    # 讀取目前 size/mtime
    stat1 = os.stat(path)
    size1, mtime1 = stat1.st_size, stat1.st_mtime
    time.sleep(stable_secs)  # 等待一小段時間，觀察是否仍在變動
    stat2 = os.stat(path)
    size2, mtime2 = stat2.st_size, stat2.st_mtime

    return (size1 == size2) and (mtime1 == mtime2)

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:

    wait_exists = FileSensor(
        task_id="wait_file_exists",
        fs_conn_id="fs_share",       # 如用 Airflow FileSystem Connection；若直接本機路徑可不填
        filepath=FILE_PATH,          # 支援通配符時，請改用 Regex/自訂邏輯
        poke_interval=POKE_SECS,
        mode="reschedule",           # 避免佔住 worker
        timeout=60 * 60,             # 最長等 1 小時，可依需求調
        soft_fail=False,
    )

    wait_unlocked = PythonSensor(
        task_id="wait_file_unlocked",
        python_callable=file_is_unlocked,
        op_kwargs={"path": FILE_PATH, "stable_secs": 4},
        poke_interval=POKE_SECS,
        mode="reschedule",
        timeout=60 * 60,
    )

    wait_exists >> wait_unlocked
