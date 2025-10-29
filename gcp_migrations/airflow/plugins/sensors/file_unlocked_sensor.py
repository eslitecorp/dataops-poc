import os, time
from airflow.sensors.base import BaseSensorOperator

class FileUnlockedSensor(BaseSensorOperator):
    """
    檢查檔案存在、可開啟，且在 stable_secs 期間 size/mtime 未變動
    以近似「寫入完成、未被鎖定」。
    """
    template_fields = ("path",)

    def __init__(self, path: str, stable_secs: int = 4, **kwargs):
        super().__init__(**kwargs)
        self.path = path
        self.stable_secs = stable_secs

    def poke(self, context):
        if not os.path.exists(self.path):
            self.log.info("File not found: %s", self.path)
            return False
        try:
            with open(self.path, "rb"):
                pass
        except OSError as e:
            self.log.info("File locked/open failed: %s (%s)", self.path, e)
            return False

        stat1 = os.stat(self.path)
        time.sleep(self.stable_secs)
        stat2 = os.stat(self.path)
        stable = (stat1.st_size == stat2.st_size) and (stat1.st_mtime == stat2.st_mtime)
        if not stable:
            self.log.info("File still changing: %s", self.path)
        return stable
