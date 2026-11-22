"""ERP CDC: Debezium topics -> bronze (change history) -> silver (current state).

For each table in TABLES, the job runs two streaming queries:

1. Kafka -> lakehouse.bronze.erp_<table>
   Writes every Debezium event as it arrives: `op` (r=snapshot, c=insert,
   u=update, d=delete), `before`, `after`, `source` (LSN, commit time...)
   and `ts_ms`, plus the Kafka origin. It is an append-only history, which
   allows auditing and rebuilding the state as of any date.

2. bronze -> lakehouse.silver.erp_<table>
   Reads the bronze table incrementally and applies the changes with MERGE,
   keeping an exact copy of the source table (deletes at the source become
   deletes here).

Each table's primary key comes from the key schema that Debezium registers in
Karapace, so it does not need to be declared here. As in the streaming job, the
schema is read at startup: if a table's structure changes at the source,
restart the job.
"""

import json
import os
import time
import urllib.error
import urllib.request

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
REGISTRY = os.getenv("SCHEMA_REGISTRY_URL", "http://karapace:8081")
TOPIC_PREFIX = os.getenv("TOPIC_PREFIX", "erp.SimulatedErp.dbo")
TABLES = [t.strip() for t in os.getenv("TABLES", "Orders,OrderItems,Stock,Payments,Shipments").split(",") if t.strip()]
BRONZE_INTERVAL = os.getenv("BRONZE_INTERVAL", "30 seconds")
SILVER_INTERVAL = os.getenv("SILVER_INTERVAL", "1 minute")
CHECKPOINTS = os.getenv("CHECKPOINTS", "/checkpoints/cdc")


def log(msg: str) -> None:
    print(f"[erp_cdc] {msg}", flush=True)


def registered_schema(subject: str) -> str:
    """Latest schema version; waits for Debezium to register it if it does not exist yet."""
    url = f"{REGISTRY}/subjects/{subject}/versions/latest"
    warned = False
    while True:
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                return json.load(resp)["schema"]
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise
        except urllib.error.URLError:
            pass
        if not warned:
            log(f"waiting for schema '{subject}' in the registry...")
            warned = True
        time.sleep(15)


def decode(column: str, avro_schema: str):
    """Messages in the Schema Registry wire format: 1 byte + 4-byte id + Avro."""
    return from_avro(F.expr(f"substring({column}, 6, length({column}) - 5)"), avro_schema)


def create_if_missing(spark: SparkSession, table: str, schema, partition=None, props=None) -> None:
    if spark.catalog.tableExists(table):
        return
    writer = spark.createDataFrame([], schema).writeTo(table).tableProperty("format-version", "2")
    for key, value in (props or {}).items():
        writer = writer.tableProperty(key, value)
    if partition is not None:
        writer = writer.partitionedBy(partition)
    writer.create()
    log(f"table {table} created")


def start_bronze(spark: SparkSession, name: str, topic: str, value_schema: str, bronze_table: str):
    events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .where(F.col("value").isNotNull())
        .select(
            decode("value", value_schema).alias("v"),
            F.col("partition").alias("_kafka_partition"),
            F.col("offset").alias("_kafka_offset"),
            F.col("timestamp").alias("_kafka_timestamp"),
            F.conv(F.hex(F.substring("value", 2, 4)), 16, 10).cast("int").alias("_schema_id"),
        )
        .select("v.op", "v.before", "v.after", "v.source", "v.ts_ms",
                "_kafka_partition", "_kafka_offset", "_kafka_timestamp", "_schema_id")
        .withColumn("_ingested_at", F.current_timestamp())
    )
    create_if_missing(
        spark, bronze_table, events.schema, partition=F.days("_ingested_at"),
        props={"write.parquet.compression-codec": "zstd"},
    )
    return (
        events.writeStream.format("iceberg")
        .queryName(f"bronze_{name}")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINTS}/{name}/bronze")
        .option("fanout-enabled", "true")
        .trigger(processingTime=BRONZE_INTERVAL)
        .toTable(bronze_table)
    )


def start_silver(spark: SparkSession, name: str, keys: list[str], bronze_table: str, silver_table: str):
    fields = [f.name for f in spark.table(bronze_table).schema["after"].dataType.fields]

    def current_state(changes: DataFrame) -> DataFrame:
        """Latest change for each key in the batch. The key defines the Kafka
        partition, so the offset gives the exact order of a key's events."""
        key_cols = [F.coalesce(F.col(f"after.{k}"), F.col(f"before.{k}")).alias(k) for k in keys]
        value_cols = [F.col(f"after.{c}").alias(c) for c in fields if c not in keys]
        latest = Window.partitionBy(*keys).orderBy(F.col("_kafka_partition"), F.col("_kafka_offset").desc())
        return (
            changes.select(
                *key_cols, *value_cols,
                F.col("op").alias("_op"),
                F.col("source.commit_lsn").alias("_lsn"),
                F.timestamp_millis(F.col("source.ts_ms")).alias("_source_changed_at"),
                F.col("_kafka_partition"), F.col("_kafka_offset"),
            )
            .withColumn("_n", F.row_number().over(latest))
            .where("_n = 1")
            .drop("_n", "_kafka_partition", "_kafka_offset")
            .withColumn("_processed_at", F.current_timestamp())
            .select(*fields, "_op", "_lsn", "_source_changed_at", "_processed_at")
        )

    empty = current_state(spark.table(bronze_table).limit(0))
    create_if_missing(spark, silver_table, empty.schema, props={
        "write.parquet.compression-codec": "zstd",
        # Frequent MERGE: writes only delete files instead of rewriting data.
        # Compacted every night by the lakehouse_maintenance DAG.
        "write.merge.mode": "merge-on-read",
        "write.update.mode": "merge-on-read",
        "write.delete.mode": "merge-on-read",
    })

    condition = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)

    def apply(batch: DataFrame, batch_id: int) -> None:
        if batch.isEmpty():
            return
        view = f"changes_{name}"
        current_state(batch).createOrReplaceTempView(view)
        batch.sparkSession.sql(f"""
            MERGE INTO {silver_table} t
            USING {view} s
            ON {condition}
            WHEN MATCHED AND s._op = 'd' THEN DELETE
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED AND s._op != 'd' THEN INSERT *
        """)

    return (
        spark.readStream.format("iceberg")
        .load(bronze_table)
        .writeStream.queryName(f"silver_{name}")
        .foreachBatch(apply)
        .option("checkpointLocation", f"{CHECKPOINTS}/{name}/silver")
        .trigger(processingTime=SILVER_INTERVAL)
        .start()
    )


def main() -> None:
    # Schemas first: without them there is nothing to process, and the cluster stays free.
    tables = {}
    for table in TABLES:
        topic = f"{TOPIC_PREFIX}.{table}"
        value_schema = registered_schema(f"{topic}-value")
        keys = [f["name"] for f in json.loads(registered_schema(f"{topic}-key"))["fields"]]
        tables[table] = (topic, value_schema, keys)
        log(f"{topic}: primary key {keys}")

    spark = SparkSession.builder.appName("erp_cdc").getOrCreate()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

    for table, (topic, value_schema, keys) in tables.items():
        name = f"erp_{table.lower()}"
        bronze, silver = f"lakehouse.bronze.{name}", f"lakehouse.silver.{name}"
        start_bronze(spark, name, topic, value_schema, bronze)
        start_silver(spark, name, keys, bronze, silver)
        log(f"{topic} -> {bronze} -> {silver}")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
