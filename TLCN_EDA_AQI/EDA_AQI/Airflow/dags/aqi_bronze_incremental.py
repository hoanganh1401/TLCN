from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {"retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="aqi_bronze_incremental",
    start_date=datetime(2025, 11, 1),
    schedule=timedelta(minutes=5),   # mỗi 5 phút
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["aqi", "bronze", "incremental"],
) as dag:

    run = BashOperator(
        task_id="bronze_incremental_hourly",
        bash_command="""
        export PYTHONPATH="${PYTHONPATH:-/opt/airflow-docker}"
        {% set lp = var.value.get('LOCATIONS_PATH') %}
        python3 -m src.etl.bronze_raw --mode incremental {{ ('--locations ' ~ lp) if lp else '' }}
        """,
        env={
            "MINIO_HOST": "{{ var.value.get('MINIO_HOST', 'minio:9000') }}",
            "MINIO_ACCESS_KEY": "{{ var.value.get('MINIO_ACCESS_KEY', 'admin') }}",
            "MINIO_SECRET_KEY": "{{ var.value.get('MINIO_SECRET_KEY', 'admin123') }}",
            "MINIO_BUCKET": "{{ var.value.get('MINIO_BUCKET', 'air-quality-raw') }}",
            "MINIO_SECURE": "{{ var.value.get('MINIO_SECURE', 'false') }}",
        },
    )

    build_global = BashOperator(
        task_id="bronze_build_global",
        bash_command="""
        export PYTHONPATH="${PYTHONPATH:-/opt/airflow-docker}"
        python3 -m src.etl.bronze_build_global
        """,
        env={
            "MINIO_HOST": "{{ var.value.get('MINIO_HOST', 'minio:9000') }}",
            "MINIO_ACCESS_KEY": "{{ var.value.get('MINIO_ACCESS_KEY', 'admin') }}",
            "MINIO_SECRET_KEY": "{{ var.value.get('MINIO_SECRET_KEY', 'admin123') }}",
            "MINIO_BUCKET": "{{ var.value.get('MINIO_BUCKET', 'air-quality-raw') }}",
            "MINIO_SECURE": "{{ var.value.get('MINIO_SECURE', 'false') }}",
        },
    )

    run >> build_global
