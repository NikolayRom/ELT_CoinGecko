FROM apache/airflow:2.8.3-python3.10

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

RUN uv pip install --system --no-cache --link-mode=copy \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.3/constraints-3.10.txt" \
    "apache-airflow-providers-celery" \
    "pyspark==3.5.1" \
    "boto3" \
    "clickhouse-connect"

RUN uv venv /opt/dbt-venv && \
    uv pip install --python /opt/dbt-venv/bin/python --no-cache \
    "dbt-clickhouse==1.8.4" "dbt-core==1.8.4" && \
    chown -R airflow:root /opt/dbt-venv

USER airflow