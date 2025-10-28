FROM apache/airflow:2.9.3

USER root
RUN apt-get update && apt-get install -y \
    curl gnupg apt-transport-https unixodbc unixodbc-dev build-essential \
 && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
 && curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list \
 && apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
 && apt-get clean && rm -rf /var/lib/apt/lists/* \
 && mkdir -p /home/airflow/.local && chown -R airflow:root /home/airflow

COPY requirements-airflow.txt requirements-airflow.txt

USER airflow
ENV PATH="/home/airflow/.local/bin:${PATH}"

# 官方 constraints 安裝：確保 airflow-core 不被破壞
RUN pip install --no-cache-dir \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.12.txt" \
      -r requirements-airflow.txt

