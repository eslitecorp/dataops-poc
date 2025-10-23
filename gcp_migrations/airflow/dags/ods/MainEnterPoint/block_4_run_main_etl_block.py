from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup

DEFAULT_ARGS = {
    "owner": "dataops",
    "retries": 0,
}

with DAG(
    dag_id="MainEnterPoint__main_etl_block",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # 先手動觸發
    catchup=False,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=6),
    tags=["ssis", "migration", "placeholder"],
) as dag:

    # ── Global endpoints ───────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    notify_failure_email = EmptyOperator(
        task_id="notify_failure_email__on_error_placeholder",
        trigger_rule=TriggerRule.ONE_FAILED,  # 任何前置失敗就會被觸發
    )

    # ── 1) 監聽與前置（watch_folder）────────────────────────────────────────────
    with TaskGroup(group_id="watch_folder") as watch_folder:
        init_vars = EmptyOperator(task_id="init_vars")
        set_param_folderpath_wql = EmptyOperator(task_id="set_param_folderpath_wql")
        watch_new_files = EmptyOperator(task_id="watch_new_files__sensor_placeholder")
        gate_prev_uploads_done = EmptyOperator(
            task_id="gate_prev_uploads_done__gate_placeholder"
        )

        init_vars >> set_param_folderpath_wql >> watch_new_files >> gate_prev_uploads_done

    # ── 2) 逐檔流程（per_file_flow）：並行 fan-out／再匯合──────────────────────
    with TaskGroup(group_id="per_file_flow") as per_file_flow:
        # 代表「展開多檔並行」的占位（之後可替換為 Dynamic Task Mapping）
        fanout_per_file = EmptyOperator(task_id="fanout_per_file__expand_placeholder")

        # 單一檔案的典型小鏈（占位）；真實版會在 dynamic mapping 內部
        set_need_force_compress = EmptyOperator(task_id="set_need_force_compress")
        complete_file_metadata = EmptyOperator(task_id="complete_file_metadata")
        enqueue_manual_review = EmptyOperator(task_id="enqueue_manual_review")
        maybe_checkfile = EmptyOperator(task_id="maybe_checkfile__branch_placeholder")
        do_checkfile = EmptyOperator(task_id="do_checkfile")

        # 匯合點：ALL_DONE 以避免前面任何 skip/失敗阻擋彙總
        files_processed_join = EmptyOperator(
            task_id="files_processed_join", trigger_rule=TriggerRule.ALL_DONE
        )

        (
            fanout_per_file
            >> set_need_force_compress
            >> complete_file_metadata
            >> enqueue_manual_review
            >> maybe_checkfile
            >> do_checkfile
            >> files_processed_join
        )

        # 側掛失敗通知（任一節點失敗就寄）
        for n in [
            fanout_per_file,
            set_need_force_compress,
            complete_file_metadata,
            enqueue_manual_review,
            maybe_checkfile,
            do_checkfile,
        ]:
            n >> notify_failure_email

    # ── 3) 彙總與通知（post_processing）───────────────────────────────────────
    with TaskGroup(group_id="post_processing") as post_processing:
        collect_pending_results = EmptyOperator(task_id="collect_pending_results")
        notify_email_summary = EmptyOperator(task_id="notify_email_summary")
        branch_matched_or_not = EmptyOperator(
            task_id="branch_matched_or_not__placeholder"
        )  # 日後可換 BranchPythonOperator

        collect_pending_results >> notify_email_summary >> branch_matched_or_not
        collect_pending_results >> notify_failure_email  # 彙總失敗也觸發通知

    # ── 4) 不符命名規則的分支（unmatched_path）────────────────────────────────
    with TaskGroup(group_id="unmatched_path") as unmatched_path:
        move_to_patternNotMatched = EmptyOperator(
            task_id="move_to_patternNotMatched"
        )
        update_FILE_REC = EmptyOperator(task_id="update_FILE_REC")

        move_to_patternNotMatched >> update_FILE_REC

    # ── 5) 符合命名規則的分支（matched_path，占位）───────────────────────────
    with TaskGroup(group_id="matched_path") as matched_path:
        matched_noop = EmptyOperator(task_id="matched_noop")

    # ── 6) 連線關係（高層級骨架）──────────────────────────────────────────────
    start >> watch_folder >> per_file_flow >> post_processing

    # 「分支」目前以占位同時接兩邊（將來換 Branch 才會真正分流）
    branch_matched_or_not >> unmatched_path >> end
    branch_matched_or_not >> matched_path >> end

    # 結束節點也應該在任何路徑都能收斂（保守做法可再加 ALL_DONE 匯合點）