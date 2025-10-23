from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id="MainEnterPoint__load_project_params",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["ssis-migration", "bootstrap", "params"],
    doc_md="""
### 系統前置作業（骨架版）
對應 SSIS 畫面：
1) 從資料表 PROJECT_PARAMS 取得參數  
2) 針對每個參數 foreach  
3) 將值設定到封裝變數  
目前全部用 **EmptyOperator** 佈線，之後逐一替換為實作節點。
""",
) as dag:

    # ---- 全局里程碑（Gate/Join） ----
    start = EmptyOperator(task_id="start")
    read_params_done = EmptyOperator(task_id="read_params_done")
    foreach_join = EmptyOperator(
        task_id="foreach_join",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS  # 任一分支跑到就可會合
    )
    end = EmptyOperator(task_id="end")

    # 1) 從資料表 PROJECT_PARAMS 取得參數（骨架）
    read_params = EmptyOperator(task_id="read_params")  # 之後換成 BQ/MySQL/SQLServer 查詢節點
    start >> read_params >> read_params_done

    # 2) foreach 參數（骨架）
    #    用 TaskGroup 表現「一組迴圈處理」，先放 start/end 兩個 gate，之後在中間塞實作
    with TaskGroup(group_id="foreach_param") as foreach_param:
        g_start = EmptyOperator(task_id="group_start")

        # === 之後你會替換成「針對單一參數的處理」 ===
        # 例如：validate_param / transform_value / map_to_pkg_var 等
        # 目前只放占位讓 UI 看得懂：
        prepare_param = EmptyOperator(task_id="prepare_param")   # e.g. 檢核/預處理
        set_pkg_var = EmptyOperator(task_id="set_pkg_variable")  # e.g. 設封裝變數

        g_end = EmptyOperator(task_id="group_end")

        g_start >> prepare_param >> set_pkg_var >> g_end

    # 3) 會合並結束
    read_params_done >> foreach_param >> foreach_join >> end
