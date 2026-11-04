from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="crypto_process_silver_and_gold",
    default_args=default_args,
    schedule=timedelta(hours=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "silver", "process", "spark", "clickhouse", "dbt"],
) as dag:

    spark_task = BashOperator(
        task_id="spark_processing",
        bash_command="python /opt/airflow/scripts/spark_transform.py --date '{{ ds }}'",
    )

    dbt_run_task = BashOperator(
        task_id='dbt_run_marts',
        bash_command="""
            cd /opt/airflow/dbt && \
            /opt/dbt-venv/bin/dbt run --profiles-dir . --no-version-check
        """,
    )

    dbt_test_task = BashOperator(
        task_id='dbt_test_data_quality',
        bash_command="""
            cd /opt/airflow/dbt && \
            /opt/dbt-venv/bin/dbt test --profiles-dir . --no-version-check
        """
    )

    spark_task >> dbt_run_task >> dbt_test_task