source .venv/bin/activate

# 建議先乾淨化（可選）
pip uninstall -y apache-airflow apache-airflow-providers-microsoft-mssql || true

# 補上 Airflow 本體
export AF_VERSION=2.9.3
export PYV=$(python -c 'import sys;print(f"{sys.version_info.major}.{sys.version_info.minor}")')

pip install "apache-airflow==${AF_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AF_VERSION}/constraints-${PYV}.txt"

# 需要 MSSQL Hook 才裝 provider（同樣帶 constraints）
pip install "apache-airflow-providers-microsoft-mssql" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AF_VERSION}/constraints-${PYV}.txt"


#export AIRFLOW_HOME="$PWD/gcp_migrations/airflow"
