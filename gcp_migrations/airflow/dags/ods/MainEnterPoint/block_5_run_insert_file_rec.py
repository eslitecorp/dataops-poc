from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

DEFAULT_ARGS = {
    "owner": "dataops",
    "depends_on_past": False,
    "retries": 0,
}

def build_ssis_mirror_dag():
    with DAG(
        dag_id="MainEnterPoint__insert_into_file_rec",
        description="Mirror SSIS flow with placeholders only (EmptyOperator).",
        default_args=DEFAULT_ARGS,
        start_date=datetime(2025, 1, 1),
        schedule=None,               # 先手動觸發；搬遷完成再排程
        catchup=False,
        tags=["ssis-migration", "mirror", "dummy"],
    ) as dag:

        # === SSIS: 順序容器 ===
        with TaskGroup(group_id="sequence_container", tooltip="對應 SSIS 的順序容器") as sequence_container:

            # 設定無需求檔案 List
            set_empty_file_list = EmptyOperator(
                task_id="set_empty_file_list",
                doc_md="**設定無需求檔案 List**（占位符）"
            )

            # === SSIS: Foreach 迴圈容器 ===
            with TaskGroup(group_id="foreach_loop", tooltip="對應 SSIS Foreach 迴圈容器") as foreach_loop:

                set_filename = EmptyOperator(
                    task_id="set_filename",
                    doc_md="**設定檔檔名**（占位符）"
                )

                insert_into_file_rec = EmptyOperator(
                    task_id="insert_into_file_rec",
                    doc_md="**insert into FILE_REC**（占位符）"
                )

                # 迴圈內部先後關係
                set_filename >> insert_into_file_rec

            # 容器內部的先後關係
            set_empty_file_list >> foreach_loop

        # 寄發遠端信件 - 資料清洗結果
        send_clean_result_email = EmptyOperator(
            task_id="send_clean_result_email",
            doc_md="**寄發遠端信件 - 資料清洗結果**（占位符）"
        )

        # 跨容器依賴：整個順序容器跑完才寄信
        sequence_container >> send_clean_result_email

        return dag

globals()["ssis_file_rec_dummy"] = build_ssis_mirror_dag()