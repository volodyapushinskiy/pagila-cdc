# Pagila CDC Pipeline

Пайплайн потоковой репликации данных (Change Data Capture), запускаемый полностью в Docker.

```
PostgreSQL → Debezium → Kafka → PySpark Streaming → ClickHouse
                                        ↑
                               Airflow запускает Spark
```

Источник данных: **Pagila** (база данных видеопроката). Отслеживаются 4 таблицы: `rental`, `payment`, `customer`, `inventory`. Изменение в PostgreSQL появляется в ClickHouse в течение ~5 секунд.

## Требования

- Docker + Docker Compose
- Git
- Bash (на Windows — Git Bash)
- 4 ГБ свободной оперативной памяти для Docker

## Быстрый старт

```bash
# 1. Клонировать репозиторий
git clone https://github.com/volodyapushinskiy/pagila-cdc.git
cd pagila-cdc

# 2. Скачать данные Pagila (~7 МБ, не хранятся в репо)
curl -L https://github.com/devrimgunduz/pagila/raw/master/pagila-data.sql -o pagila-data.sql

# 3. Создать .env
cp .env.example .env

# 4. Запустить всё
bash scripts/start.sh
```

## Запуск пайплайна

После завершения `start.sh`:

1. Открыть Airflow по адресу **http://localhost:8088** (логин: admin / пароль: admin)
2. Запустить DAG `cdc_pagila_pipeline`
3. Подождать ~2 минуты пока Spark начнёт стриминг
4. Запустить сквозной тест:

```bash
bash scripts/test_cdc.sh
```

Ожидаемый результат — изменения появляются в ClickHouse в течение 20 секунд:
```
=== ClickHouse: последние аренды ===
│ 16050 │ 1 │ 2026-05-05 07:32:16 │ u │ 2026-05-05 07:32:17.321 │

=== ClickHouse: клиент 1 ===
│ 1 │ MARY │ cdc_test@example.com │ u │ 2026-05-05 07:32:15.799 │
```

## Сервисы

| Контейнер | Порт | Роль |
|---|---|---|
| `cdc-postgres` | 5432 | База-источник (логическая репликация) |
| `cdc-zookeeper` | 2181 | Координация Kafka |
| `cdc-kafka` | 29092 | Брокер сообщений |
| `cdc-debezium` | 8083 | Kafka Connect + Debezium |
| `cdc-clickhouse` | 8123, 9000 | Аналитическое хранилище |
| `cdc-airflow` | 8088 | Оркестрация DAG |

**Интерфейсы:**
- Airflow: http://localhost:8088
- ClickHouse playground: http://localhost:8123/play
- Kafka Connect REST: http://localhost:8083/connectors

## Запросы к ClickHouse

Всегда используй `FINAL` — `ReplacingMergeTree` дедуплицирует данные лениво в фоне:

```sql
SELECT * FROM pagila_cdc.rental    FINAL ORDER BY _cdc_ts DESC LIMIT 10;
SELECT * FROM pagila_cdc.payment   FINAL ORDER BY _cdc_ts DESC LIMIT 10;
SELECT * FROM pagila_cdc.customer  FINAL ORDER BY _cdc_ts DESC LIMIT 10;
SELECT * FROM pagila_cdc.inventory FINAL ORDER BY _cdc_ts DESC LIMIT 10;
```

## Ручное тестирование

```bash
# Внести изменение в PostgreSQL
docker compose exec postgres psql -U pagila_user -d pagila -c \
  "UPDATE customer SET email='hello@example.com' WHERE customer_id=1;"

# Проверить в ClickHouse через 5 секунд
docker compose exec clickhouse clickhouse-client \
  --query "SELECT customer_id, email, _cdc_op, _cdc_ts FROM pagila_cdc.customer FINAL WHERE customer_id=1 FORMAT Pretty;"
```

## Полный сброс

```bash
docker compose down -v
```
