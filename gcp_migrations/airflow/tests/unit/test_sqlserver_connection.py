import pyodbc

SERVER = "192.168.96.13,1433"  # 例：sql01.yourcorp.local 或 10.0.0.12
DATABASE = "ODSDB"                  # 先連 master 測試
USERNAME = "TEST_CONNECTION"               # 若用 SQL 帳密
PASSWORD = "!@#$1qaz"

# ODBC Driver 18 預設要求 Encrypt；若是公司內網測試、不想管憑證，可先設 TrustServerCertificate=yes
conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={USERNAME};PWD={PASSWORD};"
    "Encrypt=yes;TrustServerCertificate=yes;"
    "Connection Timeout=5;Login Timeout=5;"
)

try:
    with pyodbc.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            print("連線成功，SELECT 1 =", cur.fetchone()[0])
except Exception as e:
    print("連線失敗：", e)