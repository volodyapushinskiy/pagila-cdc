import os
import logging
import traceback

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, coalesce, date_add, to_date, lit
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, ShortType, ByteType,
    LongType, DecimalType, TimestampType, DateType, BooleanType,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",  "kafka:9092")
CLICKHOUSE_HOST  = os.getenv("CLICKHOUSE_HOST",  "cdc-clickhouse")
CLICKHOUSE_URL   = f"jdbc:clickhouse://{CLICKHOUSE_HOST}:8123/pagila_cdc"
CLICKHOUSE_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"

spark = (
    SparkSession.builder
    .appName("PagilaCDC")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "com.clickhouse:clickhouse-jdbc:0.4.6",
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── Schemas ──────────────────────────────────────────────────────────────────

rental_schema = StructType([
    StructField("rental_id",    IntegerType()),
    StructField("rental_date",  TimestampType()),
    StructField("inventory_id", IntegerType()),
    StructField("customer_id",  ShortType()),
    StructField("return_date",  TimestampType()),
    StructField("staff_id",     ByteType()),
    StructField("updated_at",   TimestampType()),
    StructField("__op",         StringType()),
    StructField("__ts_ms",      LongType()),
])

payment_schema = StructType([
    StructField("payment_id",   IntegerType()),
    StructField("customer_id",  ShortType()),
    StructField("staff_id",     ByteType()),
    StructField("rental_id",    IntegerType()),
    StructField("amount",       DecimalType(5, 2)),
    StructField("payment_date", TimestampType()),
    StructField("updated_at",   TimestampType()),
    StructField("__op",         StringType()),
    StructField("__ts_ms",      LongType()),
])

customer_schema = StructType([
    StructField("customer_id",  ShortType()),
    StructField("store_id",     ByteType()),
    StructField("first_name",   StringType()),
    StructField("last_name",    StringType()),
    StructField("email",        StringType()),
    StructField("address_id",   ShortType()),
    # Debezium sends boolean as JSON true/false, not integer
    StructField("activebool",   BooleanType()),
    # Debezium sends DATE as integer days-since-epoch; cast to Date after parse
    StructField("create_date",  IntegerType()),
    StructField("updated_at",   TimestampType()),
    StructField("__op",         StringType()),
    StructField("__ts_ms",      LongType()),
])

inventory_schema = StructType([
    StructField("inventory_id", IntegerType()),
    StructField("film_id",      ShortType()),
    StructField("store_id",     ByteType()),
    StructField("updated_at",   TimestampType()),
    StructField("__op",         StringType()),
    StructField("__ts_ms",      LongType()),
])

# ── Sink ─────────────────────────────────────────────────────────────────────

def write_to_clickhouse(table_name):
    def _write(batch_df, batch_id):
        business_cols = [c for c in batch_df.columns if c not in ("__op", "__ts_ms")]
        not_all_null = " OR ".join(f"`{c}` IS NOT NULL" for c in business_cols)
        filtered = batch_df.filter(not_all_null)

        row_count = filtered.count()
        if row_count == 0:
            return

        out = (
            filtered
            .withColumn("_cdc_op", col("__op"))
            .withColumn("_cdc_ts", (col("__ts_ms") / 1000).cast(TimestampType()))
            .drop("__op", "__ts_ms")
            .withColumn("updated_at", coalesce(col("updated_at"), current_timestamp()))
        )

        (
            out.write
            .format("jdbc")
            .option("url", CLICKHOUSE_URL)
            .option("dbtable", table_name)
            .option("driver", CLICKHOUSE_DRIVER)
            .mode("append")
            .save()
        )
        log.info("Batch %s → %s: %d rows", batch_id, table_name, row_count)

    return _write

# ── Streams ───────────────────────────────────────────────────────────────────

def create_stream(topic, schema, table_name, post_parse=None):
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw
        .select(from_json(col("value").cast("string"), schema).alias("v"))
        .select("v.*")
    )

    if post_parse is not None:
        parsed = post_parse(parsed)

    return (
        parsed.writeStream
        .foreachBatch(write_to_clickhouse(table_name))
        .option("checkpointLocation", f"/tmp/checkpoints/{table_name}")
        .trigger(processingTime="5 seconds")
        .start()
    )

# ── Main ─────────────────────────────────────────────────────────────────────

try:
    streams = [
        create_stream("pagila.public.rental",    rental_schema,    "rental"),
        create_stream("pagila.public.payment",   payment_schema,   "payment"),
        create_stream(
            "pagila.public.customer", customer_schema, "customer",
            # Debezium sends DATE as integer days-since-epoch; convert to Date via date_add
            post_parse=lambda df: df.withColumn(
                "create_date", date_add(to_date(lit("1970-01-01")), col("create_date"))
            ),
        ),
        create_stream("pagila.public.inventory", inventory_schema, "inventory"),
    ]

    log.info("4 streaming queries started")
    spark.streams.awaitAnyTermination()

except Exception:
    log.error("Streaming job failed:\n%s", traceback.format_exc())
    raise
