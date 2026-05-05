CREATE DATABASE IF NOT EXISTS pagila_cdc;

CREATE TABLE IF NOT EXISTS pagila_cdc.rental (
    rental_id     Int32,
    rental_date   DateTime,
    inventory_id  Int32,
    customer_id   Int16,
    return_date   Nullable(DateTime),
    staff_id      Int8,
    updated_at    DateTime64(3),
    _cdc_op       LowCardinality(String),
    _cdc_ts       DateTime64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY rental_id;

CREATE TABLE IF NOT EXISTS pagila_cdc.payment (
    payment_id    Int32,
    customer_id   Int16,
    staff_id      Int8,
    rental_id     Int32,
    amount        Decimal(5,2),
    payment_date  DateTime,
    updated_at    DateTime64(3),
    _cdc_op       LowCardinality(String),
    _cdc_ts       DateTime64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY payment_id;

CREATE TABLE IF NOT EXISTS pagila_cdc.customer (
    customer_id   Int16,
    store_id      Int8,
    first_name    String,
    last_name     String,
    email         Nullable(String),
    address_id    Int16,
    activebool    UInt8,
    create_date   Date,
    updated_at    DateTime64(3),
    _cdc_op       LowCardinality(String),
    _cdc_ts       DateTime64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS pagila_cdc.inventory (
    inventory_id  Int32,
    film_id       Int16,
    store_id      Int8,
    updated_at    DateTime64(3),
    _cdc_op       LowCardinality(String),
    _cdc_ts       DateTime64(3)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY inventory_id;
