"""Scheduled load of one ERP table (SQL Server) into bronze and silver, over JDBC.

Modes:
  incremental  reads only the rows whose <change column> >= the latest mark already
               loaded (minus a lookback window, to catch transactions that committed
               late) and applies them to silver with MERGE. Does not see rows
               deleted at the source.
  full         reads the whole table and replaces silver. Sees deleted rows.
               Meant for small tables or for a periodic "reconciliation".

bronze.erp_<table>: everything that was extracted, with the load id and time.
silver.erp_<table>: current state of the source table.

Example:
  submit-job.sh erp_load.py --table Customers --key CustomerId --mode incremental
"""

import argparse
import os
import uuid
from datetime import timedelta

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def log(msg: str) -> None:
    print(f"[erp_load] {msg}", flush=True)


def read_source(spark: SparkSession, query: str):
    url = (
        f"jdbc:sqlserver://{os.environ['SQLSERVER_HOST']}:{os.environ['SQLSERVER_PORT']};"
        f"databaseName={os.environ['SQLSERVER_DB']};encrypt=true;trustServerCertificate=true;"
        "applicationName=datalake-load"
    )
    return (
        spark.read.format("jdbc")
        .option("url", url)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("user", os.environ["SQLSERVER_BATCH_USER"])
        .option("password", os.environ["SQLSERVER_BATCH_PASSWORD"])
        .option("query", query)
        .option("fetchsize", "10000")
        .load()
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", required=True, help="table name in the dbo schema")
    ap.add_argument("--key", required=True, help="primary key; several separated by commas")
    ap.add_argument("--mode", choices=["incremental", "full"], default="incremental")
    ap.add_argument("--change-column", default="UpdatedAt")
    ap.add_argument("--lookback-minutes", type=int, default=10)
    args = ap.parse_args()

    keys = [c.strip() for c in args.key.split(",")]
    name = f"erp_{args.table.lower()}"
    bronze, silver = f"lakehouse.bronze.{name}", f"lakehouse.silver.{name}"
    load_id = os.getenv("AIRFLOW_CTX_DAG_RUN_ID") or str(uuid.uuid4())

    spark = SparkSession.builder.appName(f"load_{name}").getOrCreate()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

    # 1. Extract
    query = f"SELECT * FROM dbo.[{args.table}]"
    mode = args.mode
    if mode == "incremental":
        if spark.catalog.tableExists(silver):
            mark = spark.table(silver).agg(F.max(args.change_column)).first()[0]
        else:
            mark = None
        if mark is None:
            log("silver is still empty: the first load will be full")
            mode = "full"
        else:
            since = mark - timedelta(minutes=args.lookback_minutes)
            query += f" WHERE [{args.change_column}] >= '{since:%Y-%m-%d %H:%M:%S.%f}'"
            log(f"incremental since {since} (latest mark {mark} - {args.lookback_minutes} min)")

    extracted = (
        read_source(spark, query)
        .withColumn("_load_id", F.lit(load_id))
        .withColumn("_load_mode", F.lit(mode))
        .withColumn("_extracted_at", F.current_timestamp())
        .cache()
    )
    rows = extracted.count()
    log(f"{args.table}: {rows} rows extracted ({mode})")

    # 2. Bronze: keeps everything that was extracted
    if not spark.catalog.tableExists(bronze):
        (extracted.limit(0).writeTo(bronze)
         .partitionedBy(F.days("_extracted_at"))
         .tableProperty("format-version", "2")
         .tableProperty("write.parquet.compression-codec", "zstd")
         .create())
    if rows:
        extracted.writeTo(bronze).append()

    # 3. Silver: current state
    state = extracted.drop("_load_id", "_load_mode")
    if mode == "full" or not spark.catalog.tableExists(silver):
        (state.writeTo(silver)
         .tableProperty("format-version", "2")
         .tableProperty("write.parquet.compression-codec", "zstd")
         .createOrReplace())
        log(f"{silver} replaced: {rows} rows")
    elif rows:
        latest = Window.partitionBy(*keys).orderBy(F.col(args.change_column).desc())
        (state.withColumn("_n", F.row_number().over(latest)).where("_n = 1").drop("_n")
         .createOrReplaceTempView("changes"))
        condition = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
        spark.sql(f"""
            MERGE INTO {silver} t USING changes s ON {condition}
            WHEN MATCHED AND s.`{args.change_column}` >= t.`{args.change_column}` THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        log(f"{silver}: MERGE of {rows} rows")

    log(f"silver {args.table}: {spark.table(silver).count()} rows in total")
    spark.stop()


if __name__ == "__main__":
    main()
