from __future__ import annotations
import types
import builtins
import pytest
from unittest.mock import MagicMock, patch

# 要測的函式
from gcp_migrations.airflow.plugins.mssql_utils import (
    run_scalar,
    run_query_fetchall,
    run_txn,
    call_stored_procedure,
    #bulk_insert_tsql,
    batch_insert_executemany,
)


class FakeCursor:
    def __init__(self, rows=None, fail_on=None):
        self.rows = rows or []
        self.fail_on = fail_on  # 第 N 次 execute 拋錯
        self.exec_count = 0
        self.fast_executemany = False
        self.last_executed = []
        self.closed = False

    def execute(self, sql, params=None):
        self.exec_count += 1
        self.last_executed.append((sql, tuple(params or [])))
        if self.fail_on is not None and self.exec_count == self.fail_on:
            raise RuntimeError("boom")

    def executemany(self, sql, seq_of_params):
        self.last_executed.append((sql, tuple(tuple(p) for p in seq_of_params)))

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_):
        self.closed = True


class FakeConn:
    def __init__(self, cursor: FakeCursor):
        self._cursor = cursor
        self.closed = False

    def cursor(self):
        # 模擬 pyodbc/pymssql 的 conn.cursor()
        return self._cursor

    def close(self):
        self.closed = True


@pytest.fixture
def patch_hook_get_conn(monkeypatch):
    """
    將 MsSqlHook.get_conn 攔截成回傳 FakeConn(FakeCursor)。
    每個測試可自行替換 cursor 行為。
    """
    created = {}

    def _patch(cursor: FakeCursor):
        fake_conn = FakeConn(cursor)
        def _get_conn(_self):
            return fake_conn
        #p = patch("plugins.mssql_utils.MsSqlHook.get_conn", _get_conn)
        p = patch("gcp_migrations.airflow.plugins.mssql_utils.MsSqlHook.get_conn", _get_conn)
        created["patch"] = p
        p.start()
        return fake_conn, cursor

    yield _patch
    if created.get("patch"):
        created["patch"].stop()


def test_run_scalar_success(patch_hook_get_conn):
    cursor = FakeCursor(rows=[(42,)])
    conn, _ = patch_hook_get_conn(cursor)
    val = run_scalar("SELECT COUNT(*) FROM t WHERE c=?", [1], "mssql_default")
    assert val == 42
    assert cursor.last_executed[0][0].strip().startswith("SELECT COUNT(*)")
    assert cursor.last_executed[0][1] == (1,)
    assert conn.closed is True


def test_run_query_fetchall(patch_hook_get_conn):
    rows = [("a.csv", 2), ("b.csv", 2)]
    cursor = FakeCursor(rows=rows)
    conn, _ = patch_hook_get_conn(cursor)
    got = run_query_fetchall("SELECT * FROM t", None, "mssql_default")
    assert got == rows
    assert "SELECT * FROM t" in cursor.last_executed[0][0]
    assert conn.closed is True


def test_run_txn_commit(patch_hook_get_conn):
    cursor = FakeCursor()
    conn, _ = patch_hook_get_conn(cursor)
    stmts = [
        ("UPDATE t SET c=? WHERE id=?", [2, 1]),
        ("INSERT INTO log(evt) VALUES(?)", ["OK"]),
    ]
    run_txn(stmts, "mssql_default")
    # execute 順序：BEGIN TRAN、UPDATE、INSERT、COMMIT TRAN
    sqls = [s for (s, _) in cursor.last_executed]
    assert "BEGIN TRAN" in sqls[0]
    assert "UPDATE t SET c=?" in sqls[1]
    assert "INSERT INTO log" in sqls[2]
    assert "COMMIT TRAN" in sqls[3]
    assert conn.closed is True


def test_run_txn_rollback_on_error(patch_hook_get_conn):
    # 第 3 次 execute 時故意拋錯（在 INSERT）
    cursor = FakeCursor(fail_on=3)
    conn, _ = patch_hook_get_conn(cursor)
    stmts = [
        ("UPDATE t SET c=? WHERE id=?", [2, 1]),
        ("INSERT INTO log(evt) VALUES(?)", ["OK"]),  # 這裡會 boom
    ]
    with pytest.raises(RuntimeError):
        run_txn(stmts, "mssql_default")

    sqls = [s for (s, _) in cursor.last_executed]
    assert "BEGIN TRAN" in sqls[0]
    assert "UPDATE t SET c=?" in sqls[1]
    assert "INSERT INTO log" in sqls[2]
    # 例外後應該有 ROLLBACK TRAN
    assert any("ROLLBACK TRAN" in s for s, _ in cursor.last_executed)
    assert conn.closed is True


def test_call_stored_procedure_named_params(patch_hook_get_conn):
    cursor = FakeCursor()
    conn, _ = patch_hook_get_conn(cursor)
    call_stored_procedure("dbo.usp_recalculate_metrics", {"p_date": "2025-10-28", "force": 1}, "mssql_default")

    sql, params = cursor.last_executed[0]
    assert sql.strip().startswith("EXEC dbo.usp_recalculate_metrics")
    # 具名參數轉為順序綁定
    assert tuple(params) == ("2025-10-28", 1)
    assert conn.closed is True

'''
def test_bulk_insert_tsql(patch_hook_get_conn):
    cursor = FakeCursor()
    conn, _ = patch_hook_get_conn(cursor)
    bulk_insert_tsql(
        table="dbo.StagingTable",
        file_path=r"\\shared\in\file.csv",
        with_options={"FIRSTROW": "2", "FIELDTERMINATOR": "','", "TABLOCK": ""},
        mssql_conn_id="mssql_default",
    )
    sql, params = cursor.last_executed[0]
    assert "BULK INSERT dbo.StagingTable" in sql
    # file_path 應透過參數綁定
    assert params == (r"\\shared\in\file.csv",)
    assert conn.closed is True
'''

def test_batch_insert_executemany_fast(patch_hook_get_conn):
    cursor = FakeCursor()
    conn, _ = patch_hook_get_conn(cursor)

    rows = [["f1.csv", 2], ["f2.csv", 2], ["f3.csv", 2]]
    inserted = batch_insert_executemany(
        table="dbo.FILE_REC_TMP",
        columns=["FILE_NAME", "FILE_STATUS"],
        rows=rows,
        mssql_conn_id="mssql_default",
    )

    # 有啟用 fast_executemany
    assert cursor.fast_executemany is True
    # executemany 有被呼叫
    assert "INSERT INTO dbo.FILE_REC_TMP" in cursor.last_executed[0][0]
    assert inserted == 3
    assert conn.closed is True
