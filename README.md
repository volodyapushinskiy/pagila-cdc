# Pagila CDC Pipeline

Рабочий проект: потоковый пайплайн на основе **Change Data Capture** (CDC) — технологии, которая перехватывает каждое изменение в базе данных и доставляет его в аналитическое хранилище в режиме реального времени.

Изменение в PostgreSQL появляется в ClickHouse менее чем за **5 секунд** — без опросов, без ETL-расписаний, только поток событий.

---

## Архитектура

```
PostgreSQL  ──►  Debezium  ──►  Kafka  ──►  PySpark Streaming  ──►  ClickHouse
  (источник)      (CDC)       (брокер)         (обработка)          (хранилище)
                                                     ▲
                                              Airflow (оркестрация)
```

| Компонент | Роль |
|---|---|
| **PostgreSQL 15** | Источник данных. Логическая репликация через `pgoutput` + publication `debezium_pub` |
| **Debezium 2.4** | Читает WAL PostgreSQL, публикует CDC-события в Kafka-топики |
| **Kafka 7.5** | Буфер между источником и обработчиком. Гарантирует доставку при перезапусках |
| **PySpark 3.5** | Structured Streaming: читает из Kafka батчами по 5 сек, пишет в ClickHouse через JDBC |
| **ClickHouse 23.8** | `ReplacingMergeTree` — append-only запись с ленивой дедупликацией. Запросы через `SELECT … FINAL` |
| **Airflow 2.8** | Запускает и мониторит Spark-задачу, обеспечивает рестарт при сбое |

Отслеживаются 4 таблицы базы **Pagila** (классическая учебная база видеопроката): `rental`, `payment`, `customer`, `inventory`.

---

## Требования

- Docker + Docker Compose
- Git
- Bash (на Windows — Git Bash)
- ~4 ГБ RAM для Docker

---

## Быстрый старт

```bash
# 1. Клонировать репозиторий
git clone https://github.com/volodyapushinskiy/pagila-cdc.git
cd pagila-cdc

# 2. Скачать датасет Pagila (~7 МБ, не хранится в репо)
curl -L https://github.com/devrimgunduz/pagila/raw/master/pagila-data.sql -o pagila-data.sql

# 3. Создать файл с переменными окружения
cp .env.example .env

# 4. Поднять все сервисы
bash scripts/start.sh
```

`start.sh` последовательно запускает контейнеры с нужными задержками и регистрирует Debezium-коннектор.

---

## Запуск пайплайна

После завершения `start.sh`:

1. Открыть **Airflow** → http://localhost:8088 (admin / admin)
2. Запустить DAG `cdc_pagila_pipeline`
3. Дождаться, пока задача `run_spark_streaming` перейдёт в статус Running (~2 мин)
4. Запустить тест:

```bash
bash scripts/test_cdc.sh
```

Скрипт вставляет запись в `rental`, обновляет email в `customer` и проверяет, что изменения появились в ClickHouse в течение 20 секунд.

---

## Проверка вручную

Внести изменение в PostgreSQL:

```bash
docker compose exec postgres psql -U pagila_user -d pagila -c \
  "UPDATE customer SET email='test@example.com' WHERE customer_id=1;"
```

Проверить в ClickHouse через 5 секунд:

```bash
docker compose exec clickhouse clickhouse-client \
  --query "SELECT customer_id, email, _cdc_op, _cdc_ts
           FROM pagila_cdc.customer FINAL
           WHERE customer_id = 1
           FORMAT Pretty;"
```

Поле `_cdc_op` показывает тип операции: `c` — insert, `u` — update, `d` — delete.

---

## Важно: запросы к ClickHouse

Таблицы используют движок `ReplacingMergeTree` — данные дедуплицируются лениво в фоне.
Без `FINAL` можно увидеть устаревшие версии строк.

```sql
-- Всегда добавляй FINAL
SELECT * FROM pagila_cdc.rental    FINAL ORDER BY _cdc_ts DESC LIMIT 10;
SELECT * FROM pagila_cdc.payment   FINAL ORDER BY _cdc_ts DESC LIMIT 10;
SELECT * FROM pagila_cdc.customer  FINAL ORDER BY _cdc_ts DESC LIMIT 10;
SELECT * FROM pagila_cdc.inventory FINAL ORDER BY _cdc_ts DESC LIMIT 10;
```

---

## Адреса сервисов

| Сервис | Адрес |
|---|---|
| Airflow UI | http://localhost:8088 |
| ClickHouse Playground | http://localhost:8123/play |
| Kafka Connect REST API | http://localhost:8083/connectors |

---

## Полный сброс

```bash
docker compose down -v
```

Удаляет все контейнеры и тома. При следующем запуске `start.sh` PostgreSQL загрузит данные заново.
