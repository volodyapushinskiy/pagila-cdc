from datetime import datetime, timedelta

import requests
from clickhouse_driver import Client

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "start_date": datetime(2024, 1, 1),
}

dag = DAG(
    "cdc_pagila_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["cdc", "pagila"],
)


def _check_services(**ctx):
    import logging
    log = logging.getLogger(__name__)

    resp = requests.get("http://cdc-debezium:8083/connectors", timeout=10)
    resp.raise_for_status()
    log.info("Kafka Connect connectors: %s", resp.json())

    result = Client("cdc-clickhouse").execute("SELECT 1")
    log.info("ClickHouse SELECT 1: %s", result)


def _verify_debezium_connector(**ctx):
    import logging
    log = logging.getLogger(__name__)

    resp = requests.get(
        "http://cdc-debezium:8083/connectors/pagila-connector/status",
        timeout=10,
    )

    if resp.status_code == 404:
        raise Exception(
            "Connector not registered. Run: "
            "docker compose exec kafka-connect bash /debezium/register-connector.sh"
        )

    resp.raise_for_status()
    status = resp.json()
    log.info("Connector status: %s", status)

    state = status.get("connector", {}).get("state")
    if state != "RUNNING":
        raise Exception(f"Connector state is {state}")


check_services = PythonOperator(
    task_id="check_services",
    python_callable=_check_services,
    retries=5,
    retry_delay=timedelta(seconds=30),
    dag=dag,
)

verify_debezium_connector = PythonOperator(
    task_id="verify_debezium_connector",
    python_callable=_verify_debezium_connector,
    dag=dag,
)

run_spark_streaming = BashOperator(
    task_id="run_spark_streaming",
    bash_command=(
        "/home/airflow/.local/lib/python3.8/site-packages/pyspark/bin/spark-submit "
        "--master local[2] "
        "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "com.clickhouse:clickhouse-jdbc:0.4.6 "
        "/opt/spark_jobs/streaming_job.py"
    ),
    env={
        "KAFKA_BOOTSTRAP": "kafka:9092",
        "CLICKHOUSE_HOST": "cdc-clickhouse",
    },
    dag=dag,
)

check_services >> verify_debezium_connector >> run_spark_streaming
