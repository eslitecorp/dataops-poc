from airflow.operators.python import ShortCircuitOperator
from airflow import DAG
from datetime import datetime

#from ods_main.container_process.block_1 import decide_and_prepare


DAG_ID = "Main_Container_Process"




with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ssis-mirror", "gate"],
) as dag:

    # === 第一個節點（取代 SSIS 的 C# 腳本 + 條件）===
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

    # 後面接你的 foreach tables / 狀態更新占位即可
    # decide_process_table >> next_task ...
    decide_process_table
