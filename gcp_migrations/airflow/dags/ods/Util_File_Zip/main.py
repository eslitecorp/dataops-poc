from __future__ import annotations

from datetime import datetime
from airflow import DAG
from plugins.operators.zip_and_remove import ZipAndRemoveOperator

with DAG(
    dag_id="zip_demo_dag",
    start_date=datetime(2025, 10, 1),
    schedule=None,
    catchup=False,
    params={
        # 你可以用 dagrun.conf 覆蓋這些 params
        "src_dir": "/data/inbound",
        "file_name": "ODS_ABC_20251029_CHECKFILE_20251029.csv",
        "post_name": "_POST",   # 會形成 ODS_ABC_20251029_CHECKFILE_20251029_POST.zip
        # 若你有完整路徑，也可只給 file_path，例如：/data/inbound/xxx.csv
    },
) as dag:
    zip_task = ZipAndRemoveOperator(
        task_id="zip_and_remove",
        # 兩種指定方式 (二選一)：
        # file_path="{{ dag_run.conf.get('file_path') or params.get('file_path') }}",
        src_dir="{{ dag_run.conf.get('src_dir') or params.get('src_dir') }}",
        file_name="{{ dag_run.conf.get('file_name') or params.get('file_name') }}",
        post_name="{{ dag_run.conf.get('post_name') or params.get('post_name') }}",
        overwrite=True,
        delete_original=True,
        # 如要另存目錄，可加：
        # zip_dir="/data/archive",
    )
