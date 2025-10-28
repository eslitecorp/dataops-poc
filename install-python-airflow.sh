source .venv/bin/activate
export AF_VERSION=2.9.3
export PYV=$(python -c 'import sys;print(f"{sys.version_info.major}.{sys.version_info.minor}")')
pip install "apache-airflow==${AF_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AF_VERSION}/constraints-${PYV}.txt"


export AIRFLOW_HOME="$PWD/gcp_migrations/airflow"