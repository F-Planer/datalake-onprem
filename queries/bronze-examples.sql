-- ===================================================================
-- Example queries on the BRONZE layer (Trino)
--
-- In DBeaver: open this file (File > Open File), pick the Trino
-- connection at the top of the editor, click inside a query and
-- press Ctrl+Enter to run just that one.
--
-- Bronze keeps the HISTORY: every change in the ERP is one row.
--   op = 'r' initial load | 'c' insert | 'u' update | 'd' delete
-- ===================================================================


-- 1. Latest order events (what changed, and when)
SELECT
  op,
  coalesce(after.orderid, before.orderid) AS order_id,
  before.status AS status_before,
  after.status AS status_after,
  from_unixtime(source.ts_ms / 1000) AS changed_at
FROM lakehouse.bronze.erp_orders
ORDER BY _kafka_offset DESC
LIMIT 20;


-- 2. Full history of one order (change the number)
SELECT
  op,
  before.status AS status_before,
  after.status AS status_after,
  after.totalamount AS total_amount,
  from_unixtime(source.ts_ms / 1000) AS changed_at
FROM lakehouse.bronze.erp_orders
WHERE coalesce(after.orderid, before.orderid) = 100
ORDER BY _kafka_partition, _kafka_offset;


-- 3. Items removed from orders (deletes only show up in bronze)
SELECT
  before.orderid AS order_id,
  before.itemnumber AS item_number,
  before.productid AS product_id,
  before.quantity AS quantity,
  from_unixtime(source.ts_ms / 1000) AS removed_at
FROM lakehouse.bronze.erp_orderitems
WHERE op = 'd'
ORDER BY removed_at DESC;


-- 4. Revenue by category computed from bronze ONLY.
--    For each key, take the latest event and drop the deleted ones:
--    this is the work silver already does. Must match query 5.
WITH
order_events AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY coalesce(after.orderid, before.orderid)
      ORDER BY _kafka_partition, _kafka_offset DESC
    ) AS n
  FROM lakehouse.bronze.erp_orders
),
orders AS (
  SELECT after.orderid AS orderid, after.status AS status
  FROM order_events
  WHERE n = 1 AND op <> 'd'
),
item_events AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY
        coalesce(after.orderid, before.orderid),
        coalesce(after.itemnumber, before.itemnumber)
      ORDER BY _kafka_partition, _kafka_offset DESC
    ) AS n
  FROM lakehouse.bronze.erp_orderitems
),
items AS (
  SELECT
    after.orderid AS orderid,
    after.productid AS productid,
    after.quantity AS quantity,
    after.unitprice AS unitprice
  FROM item_events
  WHERE n = 1 AND op <> 'd'
),
product_loads AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY productid
      ORDER BY updatedat DESC, _extracted_at DESC
    ) AS n
  FROM lakehouse.bronze.erp_products
),
products AS (
  SELECT productid, categoryid FROM product_loads WHERE n = 1
),
category_loads AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY categoryid
      ORDER BY _extracted_at DESC
    ) AS n
  FROM lakehouse.bronze.erp_categories
),
categories AS (
  SELECT categoryid, name FROM category_loads WHERE n = 1
)
SELECT
  c.name AS category,
  sum(i.quantity * i.unitprice) AS revenue
FROM items i
JOIN orders o ON o.orderid = i.orderid
JOIN products p ON p.productid = i.productid
JOIN categories c ON c.categoryid = p.categoryid
WHERE o.status <> 'CANCELLED'
GROUP BY c.name
ORDER BY revenue DESC;


-- 5. The same calculation on GOLD, to compare with query 4
SELECT category, sum(revenue) AS revenue
FROM lakehouse.gold.daily_sales
GROUP BY category
ORDER BY revenue DESC;
