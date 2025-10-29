from __future__ import annotations

import os
import shutil
import zipfile
from pathlib import Path
from typing import Optional

from airflow.models import BaseOperator
from airflow.utils.context import Context


class ZipAndRemoveOperator(BaseOperator):
    """
    壓縮單一檔案為 zip，檔名 = <原檔名(無副檔名)> + <post_name> + .zip
    預設壓縮成功後刪除原檔，並回傳 zip 絕對路徑到 XCom。

    Params
    ------
    src_dir : str | None
        原始檔所在資料夾（可選）。若提供 file_path 則可為 None。
    file_name : str | None
        原始檔名（可選）。若提供 file_path 則可為 None。
    file_path : str | None
        原始檔完整路徑（可選）。若同時提供 src_dir+file_name 與 file_path，優先採用 file_path。
    post_name : str
        會附加在「不含副檔名」的原檔名後面以產生 zip 名稱，例：_POST / -DONE 等。
    overwrite : bool
        若 zip 目標檔已存在，是否覆蓋（預設 True）。
    delete_original : bool
        壓縮成功後是否刪除原檔（預設 True）。
    zip_dir : str | None
        指定 zip 產出資料夾（預設與原檔相同）。

    Templated fields
    ----------------
    src_dir, file_name, file_path, post_name, zip_dir
    """

    template_fields = ("src_dir", "file_name", "file_path", "post_name", "zip_dir")

    def __init__(
        self,
        *,
        src_dir: Optional[str] = None,
        file_name: Optional[str] = None,
        file_path: Optional[str] = None,
        post_name: str = "",
        overwrite: bool = True,
        delete_original: bool = True,
        zip_dir: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.src_dir = src_dir
        self.file_name = file_name
        self.file_path = file_path
        self.post_name = post_name or ""
        self.overwrite = overwrite
        self.delete_original = delete_original
        self.zip_dir = zip_dir

    def execute(self, context: Context):
        # 1) 決定原始檔完整路徑
        if self.file_path:
            src_path = Path(self.file_path)
        else:
            if not (self.src_dir and self.file_name):
                raise ValueError("Either file_path or (src_dir and file_name) must be provided.")
            src_path = Path(self.src_dir) / self.file_name

        if not src_path.exists() or not src_path.is_file():
            raise FileNotFoundError(f"Source file not found: {src_path}")

        # 2) 建立 zip 檔名與路徑
        pre_name = src_path.stem  # 不含副檔名
        zip_name = f"{pre_name}{self.post_name}.zip"
        zip_dir = Path(self.zip_dir) if self.zip_dir else src_path.parent
        zip_dir.mkdir(parents=True, exist_ok=True)
        zip_path = (zip_dir / zip_name).resolve()

        self.log.info("Source file: %s", src_path)
        self.log.info("ZIP target : %s", zip_path)

        if zip_path.exists():
            if self.overwrite:
                self.log.warning("ZIP already exists, will overwrite: %s", zip_path)
                zip_path.unlink()
            else:
                self.log.info("ZIP already exists, overwrite=False -> returning existing file.")
                return str(zip_path)

        # 3) 原子寫入：先寫 tmp 再 rename，避免中途中斷留殘檔
        tmp_zip_path = zip_path.with_suffix(".zip.tmp")

        # 如 tmp 存在先清掉
        if tmp_zip_path.exists():
            tmp_zip_path.unlink()

        # 4) 執行壓縮（不把整個檔案讀進記憶體，直接 write）
        #   - 使用 ZIP_DEFLATED（一般通用），Python 3.7+ 可選 compresslevel；預設也可
        with zipfile.ZipFile(tmp_zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            # arcname 只放檔名，避免把整個路徑打包進 zip 內
            zf.write(src_path, arcname=src_path.name)

        # 5) 置換 tmp -> 正式檔
        tmp_zip_path.replace(zip_path)

        # 6) 刪除原檔（可選）
        if self.delete_original:
            try:
                self.log.info("Deleting original file: %s", src_path)
                src_path.unlink()
            except Exception as e:
                # 失敗不阻斷整體（但記錄）
                self.log.exception("Failed to delete original file: %s (%s)", src_path, e)

        self.log.info("ZIP created: %s", zip_path)
        return str(zip_path)  # 回傳到 XCom
