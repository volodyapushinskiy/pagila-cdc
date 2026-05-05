# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

---

## Behavioral Rules (Karpathy Guidelines)

### Don't Assume
State assumptions explicitly before acting. If a task has multiple valid interpretations, list them and ask — don't pick silently and run. If the simpler approach exists, say so. Push back when a request seems wrong.

### Don't Hide Confusion
If something is unclear, stop. Name what's confusing. Ask before writing code that might be thrown away.

### Surface Tradeoffs
When there's more than one way to do something, say so. Name the tradeoff: correctness vs speed, simplicity vs flexibility. Don't bury choices in code.

### Simplicity First
Write the minimum code that solves the problem. No speculative features. No abstractions for single-use code. No "configurability" that wasn't asked for. If a function can be 10 lines instead of 40, write 10 lines.

### Surgical Changes
Touch only what the task requires. Don't "improve" adjacent code, reformat unrelated files, or refactor things that aren't broken.

### Goal-Driven Execution
Prefer declaring success criteria over describing steps. After making a change, verify it works against those criteria before reporting done.

---

## Project Overview

CDC streaming pipeline running entirely in Docker:

```
PostgreSQL (source) → Debezium → Kafka → PySpark Streaming → ClickHouse (destination)
                                                   ↑
                                          Airflow orchestrates Spark
```

Source data: **Pagila** (DVD rental store). CDC tracks 4 tables: `rental`, `payment`, `customer`, `inventory`. A change in PostgreSQL should appear in ClickHouse within ~5–10 seconds.

---

## Key Commands

```bash
# Start everything (order matters — see scripts/start.sh for why)
bash scripts/start.sh

# Verify the pipeline end-to-end
bash scripts/test_cdc.sh

# Register Debezium connector manually (if not done by start.sh)
docker compose exec kafka-connect bash /debezium/register-connector.sh

# Check connector status
curl -s http://localhost:8083/connectors/pagila-connector/status | python3 -m json.tool

# Full reset (wipes all data)
docker compose down -v
```

**Query ClickHouse — always use FINAL:**
```bash
docker compose exec clickhouse clickhouse-client \
  --query "SELECT * FROM pagila_cdc.rental FINAL ORDER BY _cdc_ts DESC LIMIT 10 FORMAT Pretty;"
```

**Inject a test change into PostgreSQL:**
```bash
docker compose exec postgres psql -U pagila_user -d pagila -c \
  "UPDATE customer SET email='test@example.com' WHERE customer_id=1;"
```

**Run Spark job manually (from inside airflow container):**
```bash
docker compose exec airflow spark-submit \
  --master local[2] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.clickhouse:clickhouse-jdbc:0.4.6:all \
  /opt/spark_jobs/streaming_job.py
```

---

## Service Map

| Container | Port(s) | Role |
|---|---|---|
| `cdc-postgres` | 5432 | Source DB, logical replication enabled |
| `cdc-zookeeper` | 2181 | Kafka coordination |
| `cdc-kafka` | 29092 (host), 9092 (internal) | Message broker |
| `cdc-debezium` | 8083 | Kafka Connect + Debezium CDC connector |
| `cdc-clickhouse` | 8123 (HTTP), 9000 (native) | Analytics destination |
| `cdc-airflow` | 8080 | DAG orchestration |

All containers are on Docker network `cdc-net` and address each other by container name.

**UIs:**
- Airflow: http://localhost:8080 (admin / admin)
- ClickHouse playground: http://localhost:8123/play
- Kafka Connect REST: http://localhost:8083/connectors

---

## Architecture Decisions and Tradeoffs

### Why ReplacingMergeTree in ClickHouse
ClickHouse has no native `UPDATE`. `ReplacingMergeTree(updated_at)` appends every CDC event and deduplicates lazily in the background, keeping the row with the latest `updated_at`. This means:
- Writes are fast (append-only)
- **You must use `SELECT ... FINAL`** to get correct results — without it you'll see duplicate rows until the background merge runs
- Tradeoff: `FINAL` is slower on large tables. Alternative: `OPTIMIZE TABLE ... FINAL` before a query, but that's heavier.

### Why PySpark Structured Streaming over Flink/Kafka Streams
PySpark is easier to run locally in Docker without a separate cluster. Tradeoff: higher memory overhead, slower startup. For production, Flink would give better per-event latency.

### Why Airflow for a streaming job
Airflow starts and monitors the Spark job, handles restarts on failure, and provides visibility via UI. Tradeoff: Airflow is batch-native; it doesn't introspect the streaming state, only whether the process is alive.

### Kafka topic naming
Debezium auto-creates topics as `{topic.prefix}.{schema}.{table}`:
- `pagila.public.rental`
- `pagila.public.payment`
- `pagila.public.customer`
- `pagila.public.inventory`

### PostgreSQL init order
`/docker-entrypoint-initdb.d/` runs alphabetically on first container start:
1. `01-schema.sql` — Pagila table structure
2. `02-data.sql` — Pagila seed rows (~16k rentals)
3. `03-init.sql` — Adds `updated_at`, triggers, `debezium_pub` publication

Init scripts are skipped if a volume already exists. Run `docker compose down -v` to re-run them.

---

## Common Failures and Fixes

**Connector shows `FAILED`**
PostgreSQL wasn't ready when Debezium connected, or `debezium_pub` is missing.
```bash
curl -X DELETE http://localhost:8083/connectors/pagila-connector
docker compose exec kafka-connect bash /debezium/register-connector.sh
```

**No rows in ClickHouse after a change**
Check in order: (1) is the Spark job running? (2) is the connector `RUNNING`? (3) are messages in the Kafka topic?
```bash
docker compose exec cdc-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic pagila.public.rental --from-beginning --max-messages 3
```

**ClickHouse returns duplicates**
Missing `FINAL`. Always: `SELECT ... FROM pagila_cdc.rental FINAL`.

**Spark can't reach Kafka**
`streaming_job.py` uses `KAFKA_BOOTSTRAP=kafka:9092` (internal network). Outside Docker, change to `localhost:29092`.

**Scripts fail on Windows with `\r` errors**
Line endings must be LF. Fix: `dos2unix scripts/*.sh debezium/*.sh` or set `git config core.autocrlf false` before cloning.

**Airflow can't run `spark-submit`**
The `airflow/Dockerfile` installs Java 11 + PySpark. If the image predates those changes: `docker compose build --no-cache airflow`.
