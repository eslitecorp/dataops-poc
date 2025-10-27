from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# 可用 Variable/環境變數/程式計算取得要處理的 TABLE 清單
# 這裡為了示意，先放幾個假表名
TABLE_LIST = Variable.get("BUSINESS_TABLE_LIST", default_var="ORDERS,PRODUCTS,CUSTOMERS").split(",")

with DAG(
    dag_id="MainContainerProcess",
    start_date=datetime(2025, 1, 1),
    schedule=None,           # 先手動觸發，避免排程干擾
    catchup=False,
    tags=["ssis-mirror", "dummy"],
    doc_md="""
    以 EmptyOperator 鏡像 SSIS 流程：
    1) 讀取 Table 清單 → 2) 確認是否需要更新 → 3) 取得下一步參數
    4) 針對每個 TABLE：
         a. 將 TAB_STATUS 設為執行中
         b. 呼叫 PreProcess_Main（占位）
    5) 全數成功 → 將 TAB_STATUS 設為完成 → 釋放資源(狀態=未使用)
       任一失敗 → 將 TAB_STATUS 設為失敗 → 釋放資源(狀態=未使用)
    """,
) as dag:

    # 讀取 Table 清單（占位）
    load_table_list = EmptyOperator(
        task_id="load_table_list",
        doc_md="讀取可處理的 Table 名單"
    )

    # 確認當前業務是否需要執行，並將 Status 設定（占位）
    check_need_update = EmptyOperator(
        task_id="check_business_need_and_mark_status",
        doc_md="確認需要執行與否，並更新 Status 為『待執行/準備中』"
    )

    # 取得後續步驟所需參數（占位，對應灰色大框）
    prepare_next_params = EmptyOperator(
        task_id="prepare_next_step_params",
        doc_md="確認業務步驟已完成並取回後續所需參數（占位）"
    )

    # === 針對 TABLE 的 foreach 迴圈（用 TaskGroup + for 產生多個 EmptyOperator）===
    with TaskGroup(group_id="foreach_tables", tooltip="針對每個 TABLE 執行一組占位子流程") as foreach_tables:
        previous_tail = None

        for raw_name in TABLE_LIST:
            table = raw_name.strip()
            if not table:
                continue

            with TaskGroup(group_id=f"for_{table}") as for_one_table:
                mark_running = EmptyOperator(
                    task_id=f"{table}__mark_TAB_STATUS_running",
                    doc_md=f"【{table}】將 TAB_STATUS 更新為『執行中』（占位）"
                )
                run_preprocess = EmptyOperator(
                    task_id=f"{table}__run_PreProcess_Main",
                    doc_md=f"【{table}】呼叫 PreProcess_Main 啟動該表 ETL 作業（占位）"
                )
                # 線路：running → preprocess
                mark_running >> run_preprocess

            # 讓每個子組彼此平行（不串接）
            # 若你想強制序列執行，可改：previous_tail >> for_one_table
            # 這裡留白代表預設平行

    # === 匯總節點：成功與失敗兩個分支 ===
    # 「全部成功」分支起點
    all_tables_success = EmptyOperator(
        task_id="all_tables_success",
        trigger_rule=TriggerRule.ALL_SUCCESS,  # 只有全部成功才會走這裡
        doc_md="所有表處理皆成功的彙總節點（占位）"
    )

    # 「任一失敗」分支起點
    any_table_failed = EmptyOperator(
        task_id="any_table_failed",
        trigger_rule=TriggerRule.ONE_FAILED,   # 只要有失敗就會走這裡
        doc_md="任一表處理失敗的彙總節點（占位）"
    )

    # === 成功路徑 ===
    mark_all_done = EmptyOperator(
        task_id="mark_all_business_done",
        doc_md="確認所有業務表皆已完成，更新 TAB_STATUS 為『完成』（占位）"
    )
    success_release_resource = EmptyOperator(
        task_id="success_release_temp_resource",
        doc_md="釋放暫存/資源，將 STATUS 變更為『未使用』（成功路徑，占位）"
    )

    # === 失敗路徑 ===
    mark_failed = EmptyOperator(
        task_id="mark_failed_and_update_status",
        doc_md="標記當前作業為『失敗』並更新 TAB_STATUS（占位）"
    )
    failed_release_resource = EmptyOperator(
        task_id="failed_release_temp_resource",
        doc_md="釋放暫存/資源，將 STATUS 變更為『未使用』（失敗路徑，占位）"
    )

    # 線路佈局（盡量貼近 SSIS 圖的語意）
    load_table_list >> check_need_update >> prepare_next_params >> foreach_tables

    # foreach 匯總後分支
    foreach_tables >> all_tables_success
    foreach_tables >> any_table_failed

    # 成功支線
    all_tables_success >> mark_all_done >> success_release_resource

    # 失敗支線
    any_table_failed >> mark_failed >> failed_release_resource
