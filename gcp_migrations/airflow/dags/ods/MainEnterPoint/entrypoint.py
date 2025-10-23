from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.state import DagRunState
from datetime import datetime

with DAG(
    dag_id="ods_MainEnterPoint",
    start_date=datetime(2025, 1, 1),
    schedule=None,            # 手動或外部觸發
    catchup=False,
    tags=["orchestrator"],
) as dag:

    run_load_project_params = TriggerDagRunOperator(
        task_id="run_load_project_params",
        trigger_dag_id="MainEnterPoint__load_project_params",
        conf={"run_reason": "orchestrated"},
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,  # 視你的版本/需求，可保留或移除
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
    )

    run_load_user_params = TriggerDagRunOperator(
        task_id="run_load_user_params",
        trigger_dag_id="MainEnterPoint__load_user_params",
        conf={"upstream": "load_project_params_done"},
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
    )

    run_load_and_reorder = TriggerDagRunOperator(
        task_id="run_load_and_reorder",
        trigger_dag_id="MainEnterPoint__load_and_reorder",
        conf={"upstream": "load_project_params_done"},
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
    )

    run_main_etl_block = TriggerDagRunOperator(
        task_id="run_main_etl_block",
        trigger_dag_id="MainEnterPoint__main_etl_block",
        conf={"upstream": "load_user_params_done"},
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
    )

    run_insert_file_rec = TriggerDagRunOperator(
        task_id="run_insert_file_rec",
        trigger_dag_id="MainEnterPoint__insert_into_file_rec",
        conf={"upstream": "main_etl_block_done"},
        wait_for_completion=True,  # 若不需等最後一個完成可改 False
        poke_interval=30,
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
    )

    run_load_project_params >> run_load_user_params >> run_load_and_reorder >> run_main_etl_block >> run_insert_file_rec
