FROM apache/airflow:2.8.1-python3.10

USER root
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    dbt-bigquery \
    google-cloud-bigquery \
    requests \
    tenacity