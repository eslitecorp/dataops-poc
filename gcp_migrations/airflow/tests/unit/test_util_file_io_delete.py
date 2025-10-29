# tests/test_util_file_io_delete.py
import os
import shutil
import tempfile
from types import SimpleNamespace
import pytest

from gcp_migrations.airflow.dags.ods.Util_File_IO_Delete.main import _generate_src_file_path_list, _clean_old_files

@pytest.fixture
def mock_ti(monkeypatch):
    """模擬 Airflow TaskInstance 的 XCom push/pull 行為。"""
    store = {}

    class MockTI:
        def xcom_push(self, key, value):
            store[key] = value

        def xcom_pull(self, task_ids=None, key=None):
            return store.get(key)

    return MockTI()

def test_generate_src_file_path_list(mock_ti):
    """確認路徑字串能正確被解析成清單並存入 XCom。"""
    params = {"sourceFilePath": "C:\\data\\a.csv;C:\\data\\b.csv;;"}
    context = {"params": params, "ti": mock_ti}
    _generate_src_file_path_list(**context)

    result = mock_ti.xcom_pull(key="srcFilePathList")
    assert result == ["C:\\data\\a.csv", "C:\\data\\b.csv"]

def test_clean_old_files(tmp_path, mock_ti):
    """驗證 _clean_old_files 能刪除檔案與清空資料夾內容。"""
    # 建立測試資料夾與檔案
    dir_path = tmp_path / "prestage"
    dir_path.mkdir()
    file1 = dir_path / "a.txt"
    file2 = dir_path / "b.txt"
    file1.write_text("a")
    file2.write_text("b")

    subdir = dir_path / "sub"
    subdir.mkdir()
    (subdir / "nested.txt").write_text("nested")

    # 模擬 XCom: 傳入此資料夾路徑
    mock_ti.xcom_push("srcFilePathList", [str(dir_path)])

    context = {"ti": mock_ti}
    _clean_old_files(**context)

    # 資料夾本身應仍存在
    assert dir_path.exists()
    # 內容應該都被清掉
    assert not any(dir_path.iterdir())

def test_clean_old_files_empty(mock_ti):
    """當沒有任何路徑時應該直接略過不報錯。"""
    mock_ti.xcom_push("srcFilePathList", [])
    context = {"ti": mock_ti}
    _clean_old_files(**context)  # 不應拋出例外
