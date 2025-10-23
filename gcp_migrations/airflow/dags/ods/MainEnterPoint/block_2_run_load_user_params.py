from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

"""
本 DAG 以附圖 SSIS 流程為藍本，先用 EmptyOperator 搭骨架：
- TG load_system_params
    - fetch_params_from_USER_PARAMS
    - foreach_params_apply
  ↘ 若失敗 → notify_param_error_mail
- get_FOLDER_PATH_LIST_from_STAGE_TAB_ACT
- get_FILE_NAME_PATTERN_LIST_from_STAGE_TAB_ACT
- get_ZIP_FILE_NAME_PATTERN_LIST
- init_daily_job_and_prepare_folder
  ↘ 若失敗 → notify_init_error_mail

後續把 EmptyOperator 逐一替換為真實 Operator 即可。
"""

default_args = {
    "owner": "dataops",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="MainEnterPoint__load_user_params",
    description="Scaffold DAG for SSIS → Airflow migration (diagram-based).",
    start_date=datetime(2025, 10, 1),
    schedule=None,             # 先手動跑；要排程再改 cron
    catchup=False,
    default_args=default_args,
    tags=["scaffold", "ssis", "retail"],
    doc_md="""
### 說明
此 DAG 依照提供的 SSIS 視覺流程，以 **EmptyOperator** 建立 1:1 骨架。
- 兩個「失敗發信」節點以 `TriggerRule.ONE_FAILED` 表示失敗路徑。
- 後續請逐步以真實 Operator/Hook 取代對應的 EmptyOperator。
""",
) as dag:

    # ─────────────────────────────────────────────────────────────
    # TaskGroup: 讀取並套用系統參數（對應圖中上半段灰框）
    # ─────────────────────────────────────────────────────────────
    with TaskGroup(group_id="load_system_params", tooltip="從 USER_PARAMS 取得並套用系統參數") as load_system_params:
        fetch_params_from_USER_PARAMS = EmptyOperator(
            task_id="fetch_params_from_USER_PARAMS",
            doc_md="從資料庫表 USER_PARAMS 取得參數（佔位）。",
        )

        foreach_params_apply = EmptyOperator(
            task_id="foreach_params_apply",
            doc_md="針對取得的參數逐一套用（佔位，原圖為 foreach 區塊）。",
        )

        fetch_params_from_USER_PARAMS >> foreach_params_apply

    # 失敗通知（對應「設定錯誤郵件參數/MAIL」）
    notify_param_error_mail = EmptyOperator(
        task_id="notify_param_error_mail",
        trigger_rule=TriggerRule.ONE_FAILED,  # 任一 upstream 失敗就觸發
        doc_md="參數讀取/套用失敗時的通知（佔位）。",
    )

    # ─────────────────────────────────────────────────────────────
    # 主流程：依序取得路徑/檔名樣式/ZIP 檔名樣式 → 初始化每日任務與 folder
    # ─────────────────────────────────────────────────────────────
    get_FOLDER_PATH_LIST_from_STAGE_TAB_ACT = EmptyOperator(
        task_id="get_FOLDER_PATH_LIST_from_STAGE_TAB_ACT",
        doc_md="從 STAGE_TAB_ACT 取得 FOLDER_PATH 列表（佔位）。",
    )

    get_FILE_NAME_PATTERN_LIST_from_STAGE_TAB_ACT = EmptyOperator(
        task_id="get_FILE_NAME_PATTERN_LIST_from_STAGE_TAB_ACT",
        doc_md="從 STAGE_TAB_ACT 取得 FILE_NAME_PATTERN 列表（佔位）。",
    )

    get_ZIP_FILE_NAME_PATTERN_LIST = EmptyOperator(
        task_id="get_ZIP_FILE_NAME_PATTERN_LIST",
        doc_md="取得 ZIP_FILE_NAME_PATTERN 列表（佔位）。",
    )

    init_daily_job_and_prepare_folder = EmptyOperator(
        task_id="init_daily_job_and_prepare_folder",
        doc_md="(1) 檢查 STAGE_TAB_ACT 中的 FolderPath/Pattern 是否存在 "
               "(2) 建立/初始化 Daily Job 狀態 (3) 建立/準備目的資料夾（佔位）。",
    )

    # 失敗通知（對應「設定與處理錯誤郵件/MAIL」）
    notify_init_error_mail = EmptyOperator(
        task_id="notify_init_error_mail",
        trigger_rule=TriggerRule.ONE_FAILED,
        doc_md="初始化步驟失敗時的通知（佔位）。",
    )

    # ─────────────────────────────────────────────────────────────
    # 邊：與圖一致的順序與失敗路徑
    # ─────────────────────────────────────────────────────────────
    load_system_params >> get_FOLDER_PATH_LIST_from_STAGE_TAB_ACT
    load_system_params >> notify_param_error_mail  # 失敗路徑（用 ONE_FAILED）
    get_FOLDER_PATH_LIST_from_STAGE_TAB_ACT >> get_FILE_NAME_PATTERN_LIST_from_STAGE_TAB_ACT \
        >> get_ZIP_FILE_NAME_PATTERN_LIST >> init_daily_job_and_prepare_folder
    init_daily_job_and_prepare_folder >> notify_init_error_mail  # 失敗路徑（用 ONE_FAILED）
