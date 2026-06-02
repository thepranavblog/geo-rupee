FROM apache/airflow:2.8.0

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    git gcc g++ cmake && \
    rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-amazon \
    dbt-duckdb
