import os
from airflow.models import BaseOperator

class FileRenameByEkorgOperator(BaseOperator):
    """
    依 EKORG 重新命名檔案：<basename>_<ekorg><ext>
    - src_dir: 檔案所在資料夾
    - src_filename: 原始檔名（含副檔名）
    - ekorg: 若為空白/None，則不改名
    - overwrite: 目標已存在時，是否覆寫（預設 False）
    XCom: push 'res_srcRawDataFileName' 為最後檔名
    """
    template_fields = ("src_dir", "src_filename", "ekorg")

    def __init__(self, src_dir: str, src_filename: str, ekorg: str | None, overwrite: bool=False, **kwargs):
        super().__init__(**kwargs)
        self.src_dir = src_dir
        self.src_filename = src_filename
        self.ekorg = (ekorg or "").strip()
        self.overwrite = overwrite

    def execute(self, context):
        src_path = os.path.join(self.src_dir, self.src_filename)
        if not os.path.exists(src_path):
            raise FileNotFoundError(f"Source file not found: {src_path}")

        final_name = self.src_filename
        if self.ekorg:
            base, ext = os.path.splitext(self.src_filename)
            final_name = f"{base}_{self.ekorg}{ext}"

            dst_path = os.path.join(self.src_dir, final_name)
            if os.path.exists(dst_path):
                if not self.overwrite:
                    raise FileExistsError(f"Target already exists: {dst_path}")
                self.log.warning("Target exists and will be overwritten: %s", dst_path)
                os.remove(dst_path)

            self.log.info("Renaming %s -> %s", src_path, dst_path)
            os.replace(src_path, dst_path)  # 跨磁碟也安全；存在則覆蓋（前面已處理）
        else:
            self.log.info("EKORG empty; keep original name: %s", src_path)

        # 對齊 SSIS：寫入變數；在 Airflow 以 XCom 傳遞
        ti = context["ti"]
        ti.xcom_push(key="res_srcRawDataFileName", value=final_name)
        return final_name
