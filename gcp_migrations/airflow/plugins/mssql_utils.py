# plugins/mssql_utils.py
from __future__ import annotations
from typing import Any, Iterable, List, Sequence, Tuple, Dict, Optional

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


def run_scalar(sql: str, params: Optional[Sequence[Any]] = None, mssql_conn_id: str = "mssql_default") -> Any:
    """
    執行單一查詢並回傳第一列第一欄。適合 SELECT COUNT(*) 這類。
    """
    hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or [])
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()


def run_query_fetchall(sql: str, params: Optional[Sequence[Any]] = None, mssql_conn_id: str = "mssql_default") -> list:
    """
    執行查詢，回傳所有列（list of tuples）。若要 Pandas，可改用 hook.get_pandas_df。
    """
    hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or [])
            return cur.fetchall()
    finally:
        conn.close()


def run_txn(statements: Sequence[Tuple[str, Optional[Sequence[Any]]]], mssql_conn_id: str = "mssql_default") -> None:
    """
    在單一交易中依序執行多個 SQL（適合把 SSIS 裡連續 UPDATE/INSERT 合併）。
    任何一個語句丟例外就 ROLLBACK。
    """
    hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            # SQL Server 可顯式開始交易，也可依連線 autocommit 設置
            cur.execute("BEGIN TRAN")
            for sql, params in statements:
                cur.execute(sql, params or [])
            cur.execute("COMMIT TRAN")
    except Exception as e:
        try:
            with conn.cursor() as cur:
                cur.execute("ROLLBACK TRAN")
        finally:
            conn.close()
        raise e
    finally:
        try:
            conn.close()
        except Exception:
            pass


def call_stored_procedure(proc_name: str, named_params: Dict[str, Any], mssql_conn_id: str = "mssql_default") -> None:
    """
    呼叫 Stored Procedure，使用具名參數組成 EXEC 語句。
    """
    # 產出 EXEC dbo.usp_xxx @p1=?,@p2=?... 的形式 + 對應參數序列
    assignments = []
    values = []
    for k, v in named_params.items():
        assignments.append(f"@{k} = ?")
        values.append(v)

    sql = f"EXEC {proc_name} {', '.join(assignments)}"

    hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, values)
    finally:
        conn.close()

'''
def bulk_insert_tsql(
    table: str,
    file_path: str,
    with_options: Optional[Dict[str, str]] = None,
    mssql_conn_id: str = "mssql_default",
) -> None:
    """
    直接使用 T-SQL BULK INSERT（DBA 最熟悉的方式）。
    with_options 例如：{"FIRSTROW": "2", "FIELDTERMINATOR": "','", "ROWTERMINATOR": "'0x0a'", "TABLOCK": ""}
    """
    with_options = with_options or {}
    opts = []
    for k, v in with_options.items():
        if v == "":
            opts.append(k)          # 沒有等號的，比如 TABLOCK
        else:
            opts.append(f"{k} = {v}")
    opt_sql = ",\n      ".join(opts) if opts else ""

    sql = f"""
    BULK INSERT {table}
    FROM ?
    {"WITH (\n      " + opt_sql + "\n    )" if opt_sql else ""}"""

    hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, [file_path])
    finally:
        conn.close()
'''

def batch_insert_executemany(
    table: str,
    columns: Sequence[str],
    rows: Iterable[Sequence[Any]],
    mssql_conn_id: str = "mssql_default",
) -> int:
    """
    使用 executemany 做快速批次寫入，並嘗試啟用 pyodbc 的 fast_executemany（若驅動支援）。
    回傳受影響筆數（近似值）。
    """
    placeholders = ",".join(["?"] * len(columns))
    col_sql = ",".join(f"[{c}]" for c in columns)
    sql = f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders})"

    hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            # pyodbc 游標可開啟 fast_executemany，加速大量插入
            try:
                if hasattr(cur, "fast_executemany"):
                    cur.fast_executemany = True
            except Exception:
                pass

            data = list(rows)
            cur.executemany(sql, data)
            # 多數驅動在 executemany 不會精準提供 rowcount，做近似回報
            return len(data)
    finally:
        conn.close()


def get_pandas_df(sql: str, params: Optional[Sequence[Any]] = None, mssql_conn_id: str = "mssql_default"):
    """
    直接用 Hook 幫你取回 Pandas DataFrame（快速做驗證/統計）。
    """
    hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
    return hook.get_pandas_df(sql=sql, parameters=params or [])
