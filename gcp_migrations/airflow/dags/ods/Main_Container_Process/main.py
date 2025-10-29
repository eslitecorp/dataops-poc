from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow import DAG
from datetime import datetime

from ods_main.libs.main_container_process import decide_and_prepare, has_files
from ods_main.ops.main_container_process import get_do_table_list, build_sql_param_list, build_trigger_conf_list


DAG_ID = "Main_Container_Process"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    template_searchpath=["/opt/airflow/include/sql"], 
    tags=["ssis-mirror", "gate"],
) as dag:

    decide_process_table = ShortCircuitOperator(
        task_id="decide_process_table",
        python_callable=decide_and_prepare,
        
        params={
            "table_name": "ORDERS",
            "concurrent_files": "ORDERS_20251028.csv,PRODUCTS_20251028.csv",
        },
        doc_md="""
        對齊 SSIS Script Task：
        - 計算 `do_table` 與 `doTableList`
        - 更新 `file_exist_map`
        - 回傳 True/False 供 ShortCircuit 判斷是否繼續
        """,
    )

    update_stage_tab_act_prepare = MsSqlOperator(
        task_id="update_stage_tab_act_prepare",
        mssql_conn_id="mssql_ssis",
        autocommit=True,
        sql="stage/Main_Container_Process/update_stage_tab_act_prepare.sql",
        parameters=[
            "{{ dag_run.conf.get('srcNatName', params.srcNatName) }}",
            "{{ dag_run.conf.get('srcSysName', params.srcSysName) }}",
            "{{ dag_run.conf.get('srcEkorg',   params.srcEkorg)   }}",
            "{{ dag_run.conf.get('tableName',  params.tableName)  }}",
        ],
        doc_md="""
        對齊 SSIS 的 *執行 SQL 工作*：
        ```
        UPDATE [dbo].[STAGE_TAB_ACT]
        SET STATUS = 2
        WHERE NATIONALITY = ?
        AND SYS_NAME    = ?
        AND EKORG       = ?
        AND TABLE_NAME  = ?
        ```
        參數來源：`dagrun.conf`（若未提供則落回 `params`）
        """,
    )


    ensure_files_ready = ShortCircuitOperator(
        task_id="ensure_files_ready",
        python_callable=has_files,
        doc_md="若 `do_table_list` 為空則略過後續（對齊外層『確認檔案都處理完成才執行後續步驟』）",
    )


    # 取得檔名清單（XComArg）
    file_list = get_do_table_list()

    # 兩個對應的清單：SQL 參數、Trigger conf
    sql_param_list = build_sql_param_list(
        file_list=file_list,
        dag_conf="{{ dag_run.conf or {} }}",
        params="{{ params }}",
    )
    trigger_conf_list = build_trigger_conf_list(
        file_list=file_list,
        dag_conf="{{ dag_run.conf or {} }}",
        params="{{ params }}",
    )

    with TaskGroup(group_id="foreach_file_tasks", tooltip="對 do_table_list 每個檔名執行兩個步驟") as foreach_file_tasks:
        # 1) 內裏方塊1：執行參數化 SQL
        update_status_running = MsSqlOperator.partial(
            task_id="update_tab_status_running",
            mssql_conn_id="mssql_ssis",
            autocommit=True,
            sql="stage/Main_Container_Process/update_stage_tab_act_prepare__toggle_status.sql",
            doc_md="把對應鍵值的 TAB_STATUS 由 0 切到 1（執行中）",
        ).expand(parameters=sql_param_list)

        # 2) 內裏方塊2：觸發另一條 DAG（等同 SSIS PreProcess_Main 封裝）
        trigger_preprocess = TriggerDagRunOperator.partial(
            task_id="trigger_preprocess_main",
            trigger_dag_id="PreProcess_Main",     # ← 你要觸發的 DAG_ID
            wait_for_completion=True,             # 等它跑完再回來（與 SSIS 同步語意相近）
            poke_interval=30,
            reset_dag_run=True,                   # 避免同 logical_date 已有舊 run
            trigger_run_id=None,                  # 讓系統自動產生
            conf={
                # 把每個檔名與各種上下文傳給對方
                "file_name": "{{ ti.xcom_pull(task_ids='get_do_table_list')[ti.map_index] }}",
                "srcNatName": "{{ dag_run.conf.get('srcNatName', params.srcNatName) }}",
                "srcSysName": "{{ dag_run.conf.get('srcSysName', params.srcSysName) }}",
                "srcEkorg":   "{{ dag_run.conf.get('srcEkorg',   params.srcEkorg)   }}",
                "tableName":  "{{ dag_run.conf.get('tableName',  params.tableName)  }}",
            },
            doc_md="逐檔觸發 PreProcess_Main（對應 SSIS 內裏方塊 2）",
        ).expand(conf=trigger_conf_list)

        update_status_running >> trigger_preprocess


    after_foreach_all_success = EmptyOperator(
        task_id="after_foreach_all_success",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        doc_md="foreach 全數成功的彙總節點",
    )
    after_foreach_any_failed = EmptyOperator(
        task_id="after_foreach_any_failed",
        trigger_rule=TriggerRule.ONE_FAILED,
        doc_md="foreach 任一失敗的彙總節點",
    )

    
    decide_process_table >> update_stage_tab_act_prepare
    update_stage_tab_act_prepare >> ensure_files_ready >> file_list >> sql_param_list >> trigger_conf_list >> foreach_file_tasks
    
    foreach_file_tasks >> after_foreach_all_success
    foreach_file_tasks >> after_foreach_any_failed
