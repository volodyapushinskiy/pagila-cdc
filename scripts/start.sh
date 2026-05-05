#!/bin/bash
set -e
export MSYS_NO_PATHCONV=1
echo "=== Starting CDC Pipeline ==="

docker compose up -d postgres zookeeper
echo "Waiting 25s for PostgreSQL to load Pagila data..."
sleep 25

docker compose up -d kafka
echo "Waiting 20s for Kafka..."
sleep 20

docker compose up -d kafka-connect clickhouse
echo "Waiting 35s for Debezium and ClickHouse..."
sleep 35

echo "Registering Debezium connector..."
docker compose exec kafka-connect bash /debezium/register-connector.sh

docker compose up -d airflow
echo "Waiting 20s for Airflow..."
sleep 20

echo ""
echo "=== Ready ==="
echo "Airflow:    http://localhost:8080  (admin/admin)"
echo "ClickHouse: http://localhost:8123/play"
echo "Debezium:   http://localhost:8083/connectors"
echo ""
echo "Next: open Airflow, trigger DAG 'cdc_pagila_pipeline'"
echo "Then: bash scripts/test_cdc.sh"
