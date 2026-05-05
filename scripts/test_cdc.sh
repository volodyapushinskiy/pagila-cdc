#!/bin/bash
export MSYS_NO_PATHCONV=1
echo "=== Test 1: INSERT new rental ==="
docker compose exec postgres psql -U pagila_user -d pagila -c \
  "INSERT INTO rental(rental_date, inventory_id, customer_id, staff_id, updated_at)
   VALUES (NOW(), 1, 1, 1, NOW()) RETURNING rental_id;"

echo "=== Test 2: UPDATE customer email ==="
docker compose exec postgres psql -U pagila_user -d pagila -c \
  "UPDATE customer SET email='cdc_test@example.com' WHERE customer_id=1 RETURNING customer_id, email;"

echo "=== Test 3: UPDATE rental return_date ==="
docker compose exec postgres psql -U pagila_user -d pagila -c \
  "UPDATE rental SET return_date=NOW() WHERE rental_id=(SELECT MAX(rental_id) FROM rental) RETURNING rental_id, return_date;"

echo "Waiting 20s for Kafka -> Spark -> ClickHouse..."
sleep 20

echo "=== ClickHouse: latest rentals ==="
docker compose exec clickhouse clickhouse-client \
  --query "SELECT rental_id, customer_id, return_date, _cdc_op, _cdc_ts FROM pagila_cdc.rental FINAL ORDER BY _cdc_ts DESC LIMIT 5 FORMAT Pretty;"

echo "=== ClickHouse: customer 1 ==="
docker compose exec clickhouse clickhouse-client \
  --query "SELECT customer_id, first_name, email, _cdc_op, _cdc_ts FROM pagila_cdc.customer FINAL WHERE customer_id=1 FORMAT Pretty;"
