"""Late-data pattern: incremental daily aggregate of the Kafka `orders` events.

bronze.orders is partitioned by INGESTION day (_ingested_at), so a late event
(created_at days ago, arriving now) is simply appended to today's partition:
bronze never rewrites old files. The aggregate, however, is by EVENT day
(created_at), so a late event changes a day that was already computed.

Each run:
  1. reads the watermark: the latest _ingested_at already processed
     (table lakehouse.ops.watermarks);
  2. takes the bronze rows ingested after it (cheap: partition pruning) and
     collects the event days they touch. Late events bring old days;
  3. recomputes those days ENTIRELY from bronze (deduplicated by order_id,
     since Kafka delivery is at-least-once) and replaces only those
     partitions of gold.orders_daily (overwritePartitions). Untouched days
     are not rewritten;
  4. saves the new watermark, only after step 3 has committed.

If the job fails between 3 and 4, the next run recomputes the same days and
reaches the same result (idempotent). --full rebuilds everything.

The watermark is the max _ingested_at SEEN, not the clock: a micro-batch that
was still being committed during the read has a later _ingested_at and is
picked up by the next run.
"""

import argparse
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

BRONZE = os.getenv("BRONZE", "lakehouse.bronze.orders")
GOLD = os.getenv("GOLD", "lakehouse.gold.orders_daily")
WATERMARKS = "lakehouse.ops.watermarks"
JOB = "gold_orders_daily"
LATE_AFTER_MINUTES = 60   # an event is "late" if it reached Kafka more than this after created_at


def log(msg: str) -> None:
    print(f"[orders_daily] {msg}", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", action="store_true", help="rebuild every day, ignoring the watermark")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("gold_orders_daily").getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.ops")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {WATERMARKS}
                  (job STRING, watermark TIMESTAMP, updated_at TIMESTAMP) USING iceberg""")

    # 1. Watermark
    row = spark.table(WATERMARKS).where(F.col("job") == JOB).select("watermark").first()
    watermark = None if args.full or row is None else row[0]
    full = watermark is None or not spark.catalog.tableExists(GOLD)
    bronze = spark.table(BRONZE)

    # 2. New rows and the event days they touch
    new_rows = bronze if full else bronze.where(F.col("_ingested_at") > F.lit(watermark))
    summary = new_rows.agg(
        F.count("*").alias("rows"),
        F.max("_ingested_at").alias("new_watermark"),
        F.collect_set(F.to_date("created_at")).alias("days"),
        F.sum(F.when(F.to_date("created_at") < F.to_date("_ingested_at"), 1).otherwise(0)).alias("late"),
    ).first()
    if summary["rows"] == 0:
        log(f"nothing new since {watermark}")
        spark.stop()
        return
    days = sorted(summary["days"])
    log(f"{'full rebuild' if full else f'since {watermark}'}: {summary['rows']} new rows, "
        f"{summary['late']} of them for a previous day; days to recompute: {len(days)} "
        f"({days[0]} .. {days[-1]})")

    # 3. Recompute the touched days from bronze. The filter on created_at lets
    #    Iceberg skip files by their min/max statistics.
    source = bronze if full else bronze.where(F.to_date("created_at").isin(days))
    latest = Window.partitionBy("order_id").orderBy(F.col("_kafka_timestamp").desc(), F.col("_kafka_offset").desc())
    lag_minutes = (F.unix_timestamp("_kafka_timestamp") - F.unix_timestamp("created_at")) / 60
    daily = (
        source.withColumn("_n", F.row_number().over(latest)).where("_n = 1")
        .withColumn("late", lag_minutes > LATE_AFTER_MINUTES)
        .groupBy(F.to_date("created_at").alias("event_date"),
                 F.coalesce("channel", F.lit("unknown")).alias("channel"),
                 F.col("status").cast("string").alias("status"))
        .agg(
            F.count("*").alias("orders"),
            F.sum("quantity").cast("bigint").alias("items"),
            F.sum(F.col("quantity") * F.col("unit_price")).cast("decimal(16,2)").alias("revenue"),
            F.sum(F.col("late").cast("int")).cast("bigint").alias("late_orders"),
            F.round(F.max(lag_minutes) / 60, 1).alias("max_delay_hours"),
        )
        .withColumn("_computed_at", F.current_timestamp())
    )

    if full:
        (daily.writeTo(GOLD).partitionedBy(F.col("event_date"))
         .tableProperty("format-version", "2")
         .tableProperty("write.parquet.compression-codec", "zstd")
         .createOrReplace())
    else:
        daily.writeTo(GOLD).overwritePartitions()   # only the days present in `daily`
    log(f"{GOLD}: {len(days)} day(s) written")

    # 4. Watermark, only after the gold commit
    spark.createDataFrame([(JOB, summary["new_watermark"])], "job string, watermark timestamp") \
        .withColumn("updated_at", F.current_timestamp()).createOrReplaceTempView("wm")
    spark.sql(f"""MERGE INTO {WATERMARKS} t USING wm s ON t.job = s.job
                  WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
    log(f"watermark -> {summary['new_watermark']}")
    spark.stop()


if __name__ == "__main__":
    main()
