from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="ssis_prestage_to_stage_etl",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ssis", "migration", "stage"],
) as dag:

    # === 第一階段：PreStage 資料清洗與載入 ===
    start = EmptyOperator(task_id="start")

    record_log = EmptyOperator(task_id="紀錄封裝作業至_sysssislog")
    truncate_prestage = EmptyOperator(task_id="Truncate_prestage_資料表")
    bulk_insert_prestage = EmptyOperator(task_id="寫入_Prestage_Bulk_Insert")
    update_status_prestage_done = EmptyOperator(task_id="更新檔案狀態_寫入_prestage_完成")

    prestage_fail = EmptyOperator(task_id="寫入_prestage失敗_更新檔案狀態")
    judge_event_type = EmptyOperator(task_id="判斷事件類型_IU_or_D")
    get_event_list = EmptyOperator(task_id="針對檔名取得執行事件清單")
    insert_file_rec = EmptyOperator(task_id="寫入_FILE_REC")

    # === 第二階段：Stage 載入 ===
    verify_file = EmptyOperator(task_id="執行資料卡驗證")
    verify_fail = EmptyOperator(task_id="資料卡驗證失敗_更新檔案狀態")

    update_status_verified = EmptyOperator(task_id="更新檔案狀態_資料卡驗證完成")
    stage_container = EmptyOperator(task_id="導入_Stage_之交易表_rollback區塊")

    delete_before_upsert = EmptyOperator(task_id="依條件刪除舊資料_for_upsert")
    insert_stage_table = EmptyOperator(task_id="寫入_stage_資料表")

    stage_fail = EmptyOperator(task_id="導入_stage_失敗_更新狀態")
    stage_done = EmptyOperator(task_id="導入_stage_完成_資料卡業務處理")

    # === 第三階段：Stage 結束後的業務流程 ===
    update_status_stage_done = EmptyOperator(task_id="更新檔案狀態_導入_stage_完成")
    check_ready_for_next = EmptyOperator(task_id="判斷是否可導入下一層")
    update_stage_done = EmptyOperator(task_id="更新檔案狀態_導入_stage_完結")

    end = EmptyOperator(task_id="end")

    # === 流程關聯 ===
    start >> record_log >> truncate_prestage >> bulk_insert_prestage
    bulk_insert_prestage >> update_status_prestage_done
    bulk_insert_prestage >> prestage_fail

    update_status_prestage_done >> judge_event_type >> get_event_list >> insert_file_rec >> verify_file
    verify_file >> update_status_verified >> stage_container
    verify_file >> verify_fail

    stage_container >> delete_before_upsert >> insert_stage_table >> stage_done
    stage_container >> stage_fail

    stage_done >> update_status_stage_done >> check_ready_for_next >> update_stage_done >> end
