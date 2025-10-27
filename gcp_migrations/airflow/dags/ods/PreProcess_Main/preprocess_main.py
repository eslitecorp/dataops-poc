# dags/ssis_port/rebuild_file_flow.py
from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup


with DAG(
    dag_id="PreProcess_Main",
    start_date=datetime(2025, 1, 1),
    schedule=None,          # 先手動觸發
    catchup=False,
    tags=["ssis-mirror", "skeleton", "files"],
) as dag:

    start = EmptyOperator(task_id="start")

    # ────────────────────────────────────────────────────────────────────────────
    # T3: 讀取結果集/Foreach/前處理初始化（第三張圖）
    # ────────────────────────────────────────────────────────────────────────────
    with TaskGroup(group_id="t3_foreach_and_init", tooltip="第三張圖：讀取結果集 + Foreach + 前處理") as t3:

        t3_read_resultset = EmptyOperator(
            task_id="read_resultset",  # 讀取資料庫結果集值
        )

        t3_foreach_container = EmptyOperator(
            task_id="foreach_container",  # Foreach 迴圈容器（外框）
        )

        t3_set_config_to_vars = EmptyOperator(
            task_id="set_config_to_vars",  # 設定組態值到變數
        )

        t3_set_param_pooling_need_veri = EmptyOperator(
            task_id="set_param_poolingNeedVeriButWithoutCheckFileMins",  # 設定變數
        )

        t3_detect_type_and_set_vars = EmptyOperator(
            task_id="detect_file_type_and_set_following_vars",  # 【前處理】判斷檔案類型並設定後續流程使用之變數
        )

        # 兩條可能的分支：需要 CheckFile 的類型 / 一般 FILE_REC 前置參數與初始化
        t3_checkfile_type_task = EmptyOperator(
            task_id="branch_checkfile_type_task"  # Check File 檔案類型的檔案 Task
        )

        t3_set_file_rec_params = EmptyOperator(
            task_id="set_FILE_REC_required_params"  # 【前處理】設定 FILE_REC 所需參數
        )

        t3_status_init_1 = EmptyOperator(
            task_id="status_check_init_v1"  # 資料檔前處理－資料檔檢查狀態檢核與初始化 1
        )

        t3_status_init_2 = EmptyOperator(
            task_id="status_check_init_v2"  # 資料檔前處理－資料檔檢查狀態檢核與初始化 2（銜接第二張圖）
        )

        # 連線關係
        t3_read_resultset >> t3_foreach_container >> t3_set_config_to_vars >> t3_set_param_pooling_need_veri \
            >> t3_detect_type_and_set_vars

        # 分支到「需要 CheckFile」與「一般初始化」兩路
        t3_detect_type_and_set_vars >> [t3_checkfile_type_task, t3_set_file_rec_params]
        t3_set_file_rec_params >> t3_status_init_1 >> t3_status_init_2

    # ────────────────────────────────────────────────────────────────────────────
    # T2: 中繼檔、Checkfile、進入清洗（第二張圖）
    # ────────────────────────────────────────────────────────────────────────────
    with TaskGroup(group_id="t2_staging_checkfile_clean", tooltip="第二張圖：中繼檔 + Checkfile + 進入清洗") as t2:

        t2_top = EmptyOperator(
            task_id="status_check_init_v2_entry"  # （承接自 T3 的 初始化 v2）
        )

        # 左支：產製中繼檔 → 取得清單 → 狀態：已產生中繼檔 → Checkfile 檢核 → 寫入檢核結果 → 進入清洗
        t2_make_staging = EmptyOperator(
            task_id="make_staging_and_validate_col_count"  # 資料檔前處理－產製中繼檔並檢查欄位數
        )
        t2_list_staging = EmptyOperator(
            task_id="list_staging_files"  # 資料檔前處理－取得中繼檔清單
        )
        t2_mark_staging_generated = EmptyOperator(
            task_id="mark_status_staging_generated"  # 【更新檔案狀態】已產生中繼檔
        )
        t2_checkfile_verify = EmptyOperator(
            task_id="checkfile_verify"  # 資料檔前處理－Checkfile 檢核
        )
        t2_apply_checkfile_status = EmptyOperator(
            task_id="apply_checkfile_result_to_status"  # 資料檔前處理－套用 Checkfile 檢核結果之檔案狀態
        )
        t2_enter_real_cleaning = EmptyOperator(
            task_id="enter_real_cleaning"  # 進入實際資料檔的資料清洗作業
        )

        # 右支：資料檔上傳檢查（獨立並行）
        t2_upload_guard = EmptyOperator(
            task_id="upload_guard_check"  # 資料檔上傳檢查
        )

        # 關係
        t2_top >> t2_make_staging >> t2_list_staging >> t2_mark_staging_generated \
            >> t2_checkfile_verify >> t2_apply_checkfile_status >> t2_enter_real_cleaning

        t2_top >> t2_upload_guard
        # 若需要等左右皆完成再往下，可在外層加 join；這裡先保持兩路各自完成。

    # ────────────────────────────────────────────────────────────────────────────
    # T1: 無需清洗/錯誤檔搬移與刪除（第一張圖，兩邊並行）
    # ────────────────────────────────────────────────────────────────────────────
    with TaskGroup(group_id="t1_no_cleaning_or_error_paths", tooltip="第一張圖：無需清洗/錯誤檔處理與壓縮刪除") as t1:

        t1_decision_no_appoint = EmptyOperator(
            task_id="decision_no_appointed_files"  # 判斷沒有訂檔的情況（決策起點）
        )

        # 左路：第一列欄位名稱錯誤 → ongoing → error → 刪除根目錄
        t1_left_update_status = EmptyOperator(
            task_id="update_status_first_row_header_error"  # 【更新檔案狀態】第一列欄位名稱錯誤導致未產製 TT 檔
        )
        t1_left_move_ongoing_to_error = EmptyOperator(
            task_id="move_ongoing_to_error_first_row_header_error"  # 無須清洗(第一列欄位名稱錯誤)→ ongoing 至 error
        )
        t1_left_delete_root = EmptyOperator(
            task_id="delete_from_root_first_row_header_error"  # 無須清洗(第一列欄位名稱錯誤)→ 刪除根目錄檔
        )

        # 右路：無資料列需清洗 → ongoing→done → 壓縮 → 刪除根目錄
        t1_right_update_status = EmptyOperator(
            task_id="update_status_no_rows_need_clean"  # 【更新檔案狀態】無任何資料列須進行 ETL 資料清洗
        )
        t1_right_move_ongoing_to_done = EmptyOperator(
            task_id="move_ongoing_to_done"  # 無須清洗 → ongoing 至 done
        )
        t1_right_compress_done = EmptyOperator(
            task_id="compress_done_files"  # 把移至 done 之資料檔壓縮
        )
        t1_right_delete_root = EmptyOperator(
            task_id="delete_from_root_no_clean"  # 無須清洗 → 刪除根目錄檔
        )

        # 連線
        t1_decision_no_appoint >> [
            t1_left_update_status,
            t1_right_update_status,
        ]
        t1_left_update_status >> t1_left_move_ongoing_to_error >> t1_left_delete_root
        t1_right_update_status >> t1_right_move_ongoing_to_done >> t1_right_compress_done >> t1_right_delete_root

    # ────────────────────────────────────────────────────────────────────────────
    # 串接三個群組的順序（可依你實際邏輯調整）
    # 先做 T3 初始化 → T2 中繼/Checkfile/進清洗 → T1 清理不需清洗/錯誤檔的路徑
    # 若你的實際邏輯是 T1 在任何時點都可能被呼叫，也可以把它改成外側並行。
    # ────────────────────────────────────────────────────────────────────────────
    end = EmptyOperator(task_id="end")

    start >> t3 >> t2 >> t1 >> end
