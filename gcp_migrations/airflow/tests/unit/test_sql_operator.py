from airflow.operators.empty import EmptyOperator

def test_empty_operator_execution():
    task = EmptyOperator(task_id="dummy_task")
    result = task.execute(context={})
    assert result is None
