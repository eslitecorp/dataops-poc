from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

DEFAULT_ARGS = {"owner": "dataops", "depends_on_past": False, "retries": 0}

with DAG(
    dag_id="MainEnterPoint__load_and_reorder",
    description="Mirror SSIS: 系統前置作業 + 建立檔案對應 Map",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ssis-mirror", "dummy"],
) as dag:

    # 對應 SSIS 第一階段：「系統前置作業」
    system_precheck = EmptyOperator(
        task_id="system_precheck_stage_tab_act",
        doc_md="""
        **系統前置作業**  
        從資料表 `STAGE_TAB_ACT` 取得來源系統資料表的載入順序（占位符）
        """,
    )

    # 對應 SSIS 第二階段：「建立是否處理檔案對應 Map」
    build_file_mapping = EmptyOperator(
        task_id="build_file_mapping",
        doc_md="""
        **建立是否處理檔案對應 Map**  
        占位符，後續可替換成實際任務（如查表建立 Dict 或從 Config 匯入）
        """,
    )

    # 建立依賴
    system_precheck >> build_file_mapping