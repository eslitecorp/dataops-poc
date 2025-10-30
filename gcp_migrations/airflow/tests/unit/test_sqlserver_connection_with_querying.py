import pyodbc

SERVER = "192.168.96.13,1433"   # 逗號分隔埠
DATABASE = "StagingDB_TW"       # 直接連到含有 LMS.GIFT 的 DB
USERNAME = "TEST_CONNECTION"
PASSWORD = "!@#$1qaz"

conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={SERVER};DATABASE={DATABASE};"
    f"UID={USERNAME};PWD={PASSWORD};"
    "Encrypt=yes;TrustServerCertificate=yes;"
    "Connection Timeout=15;Login Timeout=15;"
)

query = """
SELECT TOP (1000)
    [GIFT_NO],
    [GIFT_NAME],
    [GIFT_NUM],
    [GIFT_COST],
    [GIFT_SAPNO],
    [GIFT_RTN_AMT],
    [S_NO],
    [GIFT_STATUS],
    [INS_DATE],
    [INS_USER],
    [UPD_DATE],
    [UPD_USER],
    [GIFT_KIND],
    [GIFT_INFO],
    [GIFT_PNT_TIMES],
    [GIFT_PNT_FIXED],
    [GIFT_DENO],
    [GIFT_DIS],
    [GIFT_CODE],
    [GIFT_SEND_DAY],
    [GIFT_SEND_PERIOD],
    [GIFT_DATE_S],
    [GIFT_DATE_E],
    [GIFT_INTRO],
    [GIFT_AMT_FIXED],
    [GIFT_TO_CMS],
    [FILE_NAME],
    [ETL_DATE],
    [DELETE_MARK]
FROM [StagingDB_TW].[LMS].[GIFT];
"""

try:
    with pyodbc.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchmany(5)  # 先看前 5 筆
            print("欄位：", cols)
            for r in rows:
                print(tuple(r))
            print(f"...共選到最多 1000 筆（僅示範列出 {len(rows)} 筆）")
except Exception as e:
    print("查詢失敗：", e)
