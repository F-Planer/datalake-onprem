"""Delivers the gold layer to the SQL Server DW (where Tableau connects).

For each gold table:
  1. writes it to stg.<table> in the DW (staging table, reloaded on every run);
  2. in a single transaction, empties dbo.<table> and copies from stg.<table>.
Whoever queries dbo.<table> never sees a half-loaded table: they see the
previous version until COMMIT, and the new one after it.

On the first load (or if the columns change in gold), dbo.<table> is created
from stg with a columnstore index, which suits Tableau's analytical queries.
The _loaded_to_dw_at column shows when the data arrived.
"""

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

DEFAULT_TABLES = "dim_product,dim_customer,fact_sales_items,daily_sales,fact_payments,fact_shipments"


def log(msg: str) -> None:
    print(f"[gold_to_dw] {msg}", flush=True)


def sqlserver_type(dtype: T.DataType) -> str:
    """Spark type -> SQL Server type. Strings become NVARCHAR(400) (not
    NVARCHAR(MAX)), which is better for indexes and for Tableau."""
    if isinstance(dtype, T.StringType):
        return "NVARCHAR(400)"
    if isinstance(dtype, T.DecimalType):
        return f"DECIMAL({dtype.precision},{dtype.scale})"
    mapping = {
        T.IntegerType: "INT", T.LongType: "BIGINT", T.ShortType: "SMALLINT", T.ByteType: "TINYINT",
        T.BooleanType: "BIT", T.DoubleType: "FLOAT", T.FloatType: "REAL",
        T.DateType: "DATE", T.TimestampType: "DATETIME2(6)", T.TimestampNTZType: "DATETIME2(6)",
    }
    for cls, name in mapping.items():
        if isinstance(dtype, cls):
            return name
    raise ValueError(f"no SQL Server mapping for type: {dtype}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tables", default=DEFAULT_TABLES)
    args = ap.parse_args()

    url = (
        f"jdbc:sqlserver://{os.environ['SQLSERVER_HOST']}:{os.environ['SQLSERVER_PORT']};"
        f"databaseName={os.environ['SQLSERVER_DW_DB']};encrypt=true;trustServerCertificate=true;"
        "applicationName=datalake-delivery;useBulkCopyForBatchInsert=true"
    )
    user, password = os.environ["SQLSERVER_DW_USER"], os.environ["SQLSERVER_DW_PASSWORD"]

    spark = SparkSession.builder.appName("gold_to_dw").getOrCreate()
    jvm = spark.sparkContext._jvm
    connection = jvm.java.sql.DriverManager.getConnection(url, user, password)

    def execute(sql: str) -> None:
        stmt = connection.createStatement()
        try:
            stmt.execute(sql)
        finally:
            stmt.close()

    def columns(schema: str, table: str) -> list[str]:
        stmt = connection.prepareStatement(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION")
        stmt.setString(1, schema)
        stmt.setString(2, table)
        rs = stmt.executeQuery()
        names = []
        while rs.next():
            names.append(rs.getString(1))
        stmt.close()
        return names

    try:
        for table in [t.strip() for t in args.tables.split(",") if t.strip()]:
            df = spark.table(f"lakehouse.gold.{table}").withColumn("_loaded_to_dw_at", F.current_timestamp())
            definition = ", ".join(f"[{c.name}] {sqlserver_type(c.dataType)}" for c in df.schema)

            # Creates (or recreates, if the columns changed in gold) stg and dbo with explicit types.
            if columns("dbo", table) != df.columns or columns("stg", table) != df.columns:
                execute(f"DROP TABLE IF EXISTS stg.{table}")
                execute(f"DROP TABLE IF EXISTS dbo.{table}")
                execute(f"CREATE TABLE stg.{table} ({definition})")
                execute(f"CREATE TABLE dbo.{table} ({definition})")
                execute(f"CREATE CLUSTERED COLUMNSTORE INDEX cci_{table} ON dbo.{table}")
                log(f"stg.{table} and dbo.{table} created (dbo with columnstore)")

            # Reloads stg: overwrite + truncate empties the table instead of recreating it.
            (df.write.format("jdbc").mode("overwrite").option("truncate", "true")
             .option("url", url).option("user", user).option("password", password)
             .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
             .option("dbtable", f"stg.{table}").option("batchsize", "10000")
             .save())
            rows = df.count()

            execute(f"""
                SET XACT_ABORT ON;
                BEGIN TRANSACTION;
                    TRUNCATE TABLE dbo.{table};
                    -- MAXDOP 1: a parallel insert into columnstore can deadlock with itself
                    INSERT INTO dbo.{table} WITH (TABLOCK) SELECT * FROM stg.{table} OPTION (MAXDOP 1);
                COMMIT;
            """)
            log(f"dbo.{table}: {rows} rows delivered")
    finally:
        connection.close()
        spark.stop()


if __name__ == "__main__":
    main()
