"""Streaming: Kafka topic `orders` (Avro via Karapace) -> lakehouse.bronze.orders (Iceberg).

Messages arrive in the Schema Registry wire format: 1 "magic" byte (0) +
4 bytes with the schema id + the Avro body. The job fetches the schema from
Karapace, drops the first 5 bytes and decodes the rest with Spark's from_avro.
That way it does not depend on Confluent libraries.

POC limitation: the schema is read once, at startup. If the producer switches
to a new schema version, the job must be restarted.
"""

import json
import os
import time
import urllib.error
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
REGISTRY = os.getenv("SCHEMA_REGISTRY_URL", "http://karapace:8081")
TOPIC = os.getenv("TOPIC", "orders")
TABLE = os.getenv("TABLE", "lakehouse.bronze.orders")
CHECKPOINT = os.getenv("CHECKPOINT", "/checkpoints/bronze_orders")
INTERVAL = os.getenv("INTERVAL", "30 seconds")


def latest_schema(subject: str) -> tuple[int, str]:
    """Fetches the latest schema version; waits while it does not exist yet
    (the subject is only created when the producer sends its first message)."""
    url = f"{REGISTRY}/subjects/{subject}/versions/latest"
    warned = False
    while True:
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                data = json.load(resp)
            return data["id"], data["schema"]
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise
        except urllib.error.URLError:
            pass
        if not warned:
            print(f"[bronze_orders] waiting for schema '{subject}' in the registry...", flush=True)
            warned = True
        time.sleep(15)


def main() -> None:
    # Before creating the Spark session, so the cluster is not held while waiting.
    schema_id, avro_schema = latest_schema(f"{TOPIC}-value")
    print(f"[bronze_orders] schema {TOPIC}-value id={schema_id}", flush=True)

    spark = SparkSession.builder.appName("bronze_orders").getOrCreate()

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    orders = raw.select(
        from_avro(F.expr("substring(value, 6, length(value) - 5)"), avro_schema).alias("v"),
        F.col("topic").alias("_kafka_topic"),
        F.col("partition").alias("_kafka_partition"),
        F.col("offset").alias("_kafka_offset"),
        F.col("timestamp").alias("_kafka_timestamp"),
        F.conv(F.hex(F.substring("value", 2, 4)), 16, 10).cast("int").alias("_schema_id"),
    ).select("v.*", "_kafka_topic", "_kafka_partition", "_kafka_offset", "_kafka_timestamp", "_schema_id")
    orders = orders.withColumn("_ingested_at", F.current_timestamp())

    # Creates the table on the first run, partitioned by ingestion day.
    if not spark.catalog.tableExists(TABLE):
        (
            spark.createDataFrame([], orders.schema)
            .writeTo(TABLE)
            .partitionedBy(F.days("_ingested_at"))
            .tableProperty("format-version", "2")
            .tableProperty("write.parquet.compression-codec", "zstd")
            .create()
        )
        print(f"[bronze_orders] table {TABLE} created", flush=True)

    query = (
        orders.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT)
        .option("fanout-enabled", "true")
        .trigger(processingTime=INTERVAL)
        .toTable(TABLE)
    )
    print(f"[bronze_orders] writing {TOPIC} -> {TABLE} every {INTERVAL}", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
