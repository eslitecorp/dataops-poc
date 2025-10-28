def test_dag_integrity(dag_bag):
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"

def test_all_dags_have_tags(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        assert dag.tags, f"{dag_id} has no tags"
