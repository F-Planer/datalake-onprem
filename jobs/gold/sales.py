"""Sales gold layer: star schema for Tableau, built from silver.

Tables (fully rebuilt on every run; each swap is atomic in Iceberg):
  gold.dim_product        product + category, current price
  gold.dim_customer       customer without personal data (no e-mail)
  gold.fact_sales_items   one row per order item
  gold.daily_sales        aggregated by day, category, state and channel (no cancelled orders)
  gold.fact_payments      one row per payment (method, status, amount)
  gold.fact_shipments     one row per shipment, with carrier and lead times in hours

Lowercase names with underscores, which behave the same in Trino, Spark and
SQL Server.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

S = "lakehouse.silver"
G = "lakehouse.gold"


def write(df, table: str) -> None:
    (df.writeTo(f"{G}.{table}")
     .tableProperty("format-version", "2")
     .tableProperty("write.parquet.compression-codec", "zstd")
     .createOrReplace())
    print(f"[gold_sales] {G}.{table}: {df.sparkSession.table(f'{G}.{table}').count()} rows", flush=True)


def main() -> None:
    spark = SparkSession.builder.appName("gold_sales").getOrCreate()
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {G}")

    categories = spark.table(f"{S}.erp_categories")
    products = spark.table(f"{S}.erp_products")
    customers = spark.table(f"{S}.erp_customers")
    orders = spark.table(f"{S}.erp_orders")
    items = spark.table(f"{S}.erp_orderitems")

    dim_product = (
        products.join(categories, "CategoryId", "left")
        .select(
            F.col("ProductId").alias("product_id"),
            F.col("Sku").alias("sku"),
            products["Name"].alias("product"),
            categories["Name"].alias("category"),
            F.col("Price").alias("current_price"),
            F.col("Active").alias("active"),
        )
    )
    write(dim_product, "dim_product")

    dim_customer = customers.select(
        F.col("CustomerId").alias("customer_id"),
        F.col("Name").alias("customer"),
        F.col("City").alias("city"),
        F.col("State").alias("state"),
        F.to_date("CreatedAt").alias("customer_since"),
    )
    write(dim_customer, "dim_customer")

    fact = (
        items.join(orders, "OrderId")
        .select(
            F.col("OrderId").alias("order_id"),
            F.col("ItemNumber").alias("item_number"),
            F.to_date(orders["CreatedAt"]).alias("order_date"),
            orders["CreatedAt"].alias("created_at"),
            F.col("CustomerId").alias("customer_id"),
            F.col("ProductId").alias("product_id"),
            F.col("Status").alias("status"),
            F.coalesce(F.col("Channel"), F.lit("unknown")).alias("channel"),
            F.col("Quantity").alias("quantity"),
            F.col("UnitPrice").alias("unit_price"),
            (F.col("Quantity") * F.col("UnitPrice")).cast("decimal(14,2)").alias("item_amount"),
        )
    )
    write(fact, "fact_sales_items")

    daily_sales = (
        spark.table(f"{G}.fact_sales_items").where("status <> 'CANCELLED'")
        .join(spark.table(f"{G}.dim_product"), "product_id")
        .join(spark.table(f"{G}.dim_customer"), "customer_id")
        .groupBy("order_date", "category", "state", "channel")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.sum("quantity").alias("items_sold"),
            F.sum("item_amount").cast("decimal(16,2)").alias("revenue"),
        )
    )
    write(daily_sales, "daily_sales")

    payments = spark.table(f"{S}.erp_payments")
    fact_payments = payments.select(
        F.col("PaymentId").alias("payment_id"),
        F.col("OrderId").alias("order_id"),
        F.to_date("PaidAt").alias("payment_date"),
        F.col("PaidAt").alias("paid_at"),
        F.col("Method").alias("method"),
        F.col("Status").alias("status"),
        F.col("Amount").alias("amount"),
    )
    write(fact_payments, "fact_payments")

    shipments = spark.table(f"{S}.erp_shipments")
    carriers = spark.table(f"{S}.erp_carriers")

    def hours(start, end):
        return F.round((F.unix_timestamp(end) - F.unix_timestamp(start)) / 3600, 1)

    fact_shipments = (
        shipments.join(orders.select("OrderId", "CreatedAt"), "OrderId", "left")
        .join(carriers.select("CarrierId", F.col("Name").alias("carrier"), "ServiceLevel"),
              "CarrierId", "left")
        .select(
            F.col("ShipmentId").alias("shipment_id"),
            F.col("OrderId").alias("order_id"),
            F.to_date("CreatedAt").alias("order_date"),
            F.col("carrier"),
            F.col("ServiceLevel").alias("service_level"),
            F.col("ShippedAt").alias("shipped_at"),
            F.col("DeliveredAt").alias("delivered_at"),
            hours(F.col("CreatedAt"), F.col("ShippedAt")).alias("hours_to_ship"),
            hours(F.col("ShippedAt"), F.col("DeliveredAt")).alias("hours_to_deliver"),
            F.col("DeliveredAt").isNotNull().alias("delivered"),
        )
    )
    write(fact_shipments, "fact_shipments")
    spark.stop()


if __name__ == "__main__":
    main()
