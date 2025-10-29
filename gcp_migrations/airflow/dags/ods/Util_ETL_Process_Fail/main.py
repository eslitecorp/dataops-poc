from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "util_move_to_error_flow"

def _conf(**kwargs):
    """統一把 dag_run.conf 與 params 合併傳給子DAG。"""
    # 可依需要擴充：file_name、paths、execution ids...
    return {
        "file_name": "{{ dag_run.conf.get('file_name', params.get('file_name')) }}",
        "src_dir": "{{ dag_run.conf.get('src_dir', params.get('src_dir')) }}",
        "error_dir": "{{ dag_run.conf.get('error_dir', params.get('error_dir')) }}",
    }

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule=None,   # 手動或由上游觸發
    catchup=False,
    tags=["ssis-migration", "all-trigger"],
    description="更新狀態 → 刪除T檔(等待結果) → 成功/失敗分流，各自觸發對應的搬檔DAG",
) as dag:
    # 1) 更新檔案狀態（觸發另一個DAG）
    t_update_status = TriggerDagRunOperator(
        task_id="trigger__更新檔案狀態",
        trigger_dag_id="util_update_file_status",   # ← 你的「更新檔案狀態」DAG 名稱
        conf=_conf(),
        reset_dag_run=True,
        wait_for_completion=True,    # 視需要而定；若不在此分流，可改 False
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
        doc_md="觸發【更新檔案狀態】DAG，預設等待完成。",
    )

    # 2) 刪除 ongoing/ 下的 T 檔（必須等待以決定後續分支）
    t_delete_tfile = TriggerDagRunOperator(
        task_id="trigger__刪除_T檔",
        trigger_dag_id="util_delete_tfile",         # ← 你的「刪除T檔」DAG 名稱
        conf=_conf(),
        reset_dag_run=True,
        wait_for_completion=True,    # **要分流，一定要等結果**
        poke_interval=30,
        allowed_states=["success"],  # 子DAG成功 => 本task成功
        failed_states=["failed", "skipped"],  # 子DAG失敗/跳過 => 本task失敗
        doc_md="觸發【刪除T檔】DAG；成功/失敗用於之後的分流。",
    )

    # 3-L) （左）成功路徑：刪除T檔成功 → 搬到 error（含T檔情境）
    t_move_with_t = TriggerDagRunOperator(
        task_id="trigger__搬到error__含T檔情境",
        trigger_dag_id="util_move_files_to_error_with_tfile",  # ← 你的子DAG
        conf=_conf(),
        reset_dag_run=True,
        wait_for_completion=False,
        trigger_rule=TriggerRule.ALL_SUCCESS,  # 上游（刪除T檔）成功才會觸發
        doc_md="刪除T檔成功才會觸發此DAG。",
    )

    # 3-R) （右）失敗路徑：刪除T檔失敗 → 搬到 error（無T檔情境）
    t_move_without_t = TriggerDagRunOperator(
        task_id="trigger__搬到error__無T檔情境",
        trigger_dag_id="util_move_files_to_error_without_tfile",  # ← 你的子DAG
        conf=_conf(),
        reset_dag_run=True,
        wait_for_completion=False,
        trigger_rule=TriggerRule.ALL_FAILED,  # 上游（刪除T檔）失敗才會觸發
        doc_md="刪除T檔失敗（或未找到T檔）才會觸發此DAG。",
    )

    # 串接
    t_update_status >> t_delete_tfile
    t_delete_tfile >> [t_move_with_t, t_move_without_t]
