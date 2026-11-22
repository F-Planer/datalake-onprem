# Operations manual

A step-by-step guide to running the data lake day to day: where to look,
how to add or remove tables, what to do when something fails, test data and
maintenance. Every procedure here was run and verified on the stack. The
architecture is described in [`README.md`](../README.md).

Every command runs from the project folder (the cloned repository):

```bash
cd datalake-onprem
```

---

## 1. Seeing what is going on

### Quick check (always start here)

```bash
./scripts/check.sh
```

In about 5 seconds, the script shows:

- stopped or unhealthy containers;
- the state of the CDC connector;
- the jobs running on Spark;
- the latest Airflow runs;
- the most recent data in the lake and in the DW;
- disk space.

Everything marked `[ok]` is fine. Lines marked `[!!]` tell you where to look.

### Where to look, by question

| Question | Where to look |
|---|---|
| Did the scheduled loads run? Did any fail? | **Airflow**, http://localhost:8095 → DAGs → click the DAG → green squares (success) or red (failure). Click a square → **Logs** |
| Is CDC capturing changes? | **Kafka UI**, http://localhost:8085 → **Kafka Connect** → connector `erp-sqlserver` must be `RUNNING` |
| Are CDC messages arriving? | **Kafka UI** → **Topics** → `erp.<database>.dbo.<Table>` → **Messages** tab |
| Are the continuous Spark jobs up? | **Spark**, http://localhost:8090 → *Running Applications* must list `bronze_orders` and `erp_cdc` |
| Is the CDC job processing? | http://localhost:4041 → **Structured Streaming** tab (each table has a bronze and a silver query) |
| What is in the lake? Do the numbers match? | **DBeaver** → Trino connection (`localhost:8088`). Examples in `queries/` |
| What reached the DW? | **DBeaver** → SQL Server connection → database `AnalyticsDW`, schema `dbo` |
| I want to explore data with Python/Spark | **JupyterLab**, http://localhost:8899 (token: `grep JUPYTER_TOKEN .env`) |
| Which queries ran on Trino? | **Trino**, http://localhost:8088 (monitoring only, no editor) |
| Physical files in storage | **SeaweedFS**, http://localhost:8888 → `buckets/lakehouse` |

### Basic Docker commands

| To | Command |
|---|---|
| See every container and its state | `docker compose ps -a` |
| See a service's log (last 10 min) | `docker compose logs --since 10m spark-cdc` |
| Follow the log live (Ctrl+C to exit) | `docker compose logs -f spark-cdc` |
| Restart a service | `docker compose restart spark-cdc` |
| Start everything after rebooting the PC | see [Start everything](#start-everything) |
| Memory and CPU of each container | `docker stats --no-stream` |

Service names, for use in those commands:

- **Base:** `postgres`, `seaweedfs`, `polaris`.
- **Streaming:** `kafka`, `karapace`, `kafka-ui`.
- **Spark:** `spark-master`, `spark-worker`, `spark-streaming-bronze`.
- **CDC:** `kafka-connect`, `spark-cdc`.
- **Simulated source:** `sqlserver`.
- **Airflow:** `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`.
- **Query and notebook:** `trino`, `jupyter`.

### Start everything

```bash
docker compose --profile streaming --profile simulated-source \
  --profile cdc --profile batch --profile query \
  --profile notebook up -d
```

With a real SQL Server as the source, drop `--profile simulated-source` and
point the `SQLSERVER_*` variables in `.env` to it.

---

## 2. Adding a new table to the data lake

### First: CDC or scheduled load?

| Use **CDC** when | Use a **scheduled load** when |
|---|---|
| You need the data within 1–2 minutes | 1 hour (or 1 day) of delay is acceptable |
| The table changes a lot during the day | The table is small or changes little |
| You need the history of every change | Only the current state matters |
| Deletes at the source must show up quickly | Deletes can show up the next day |

CDC costs more on SQL Server: the Agent keeps reading the transaction log and
the `cdc.*` tables grow. When in doubt, start with the scheduled load.

Either way, the table **must have a primary key**.

In the examples below, the new table is called `Suppliers`, with key
`SupplierId`, and the source database is `SimulatedErp`. Replace them with the
real names.

### 2A. Scheduled load (Airflow)

**Step 1. Read permission on SQL Server.** Run by the DBA, or by you with an
admin login, in DBeaver or SSMS:

```sql
USE SimulatedErp;
GRANT SELECT ON dbo.Suppliers TO batch_reader;
```

**Step 2. Add the table to the DAG.** Open `airflow/dags/erp_sales.py` and
add a line to one of these two blocks:

```python
HOURLY_LOADS = {           # incremental every hour + full every day
    "Customers": "CustomerId",
    "Products": "ProductId",
    "Suppliers": "SupplierId",     # <- new line
}
DAILY_LOADS = {            # full only, once a day
    "Categories": "CategoryId",
}
```

- **`HOURLY_LOADS`** requires the `UpdatedAt` column, with the date and time of
  the last change. If the column has another name, the job takes
  `--change-column` (see `jobs/batch/erp_load.py`).
- **`DAILY_LOADS`** is for tables without that column, or small ones.
- **Composite key:** `"InvoiceItems": "InvoiceId,ItemNumber"`.

**Step 3. Wait for Airflow to read the file.** It takes about 30 seconds. To
check that the file has no errors:

```bash
docker exec datalake-airflow-scheduler-1 airflow dags list-import-errors
```

If it prints `No data found`, all is well.

**Step 4. Run the first load without waiting for the schedule.**

1. In Airflow, open the `erp_sales_daily` DAG.
2. Click **Trigger**.
3. Wait for the squares to turn green.

The first load is always full.

**Step 5. Check in DBeaver (Trino):**

```sql
SELECT count(*) FROM lakehouse.silver.erp_suppliers;
```

Compare with `SELECT count(*) FROM dbo.Suppliers` on SQL Server.

### 2B. CDC (near real time)

**Step 1. Enable CDC on the table and grant permission.** Run by the DBA, who
must be `db_owner`:

```sql
USE SimulatedErp;
EXEC sys.sp_cdc_enable_table
     @source_schema = N'dbo',
     @source_name   = N'Suppliers',
     @role_name     = N'cdc_reader',
     @supports_net_changes = 1;
GRANT SELECT ON dbo.Suppliers TO cdc_reader;
```

**Step 2. Add it to the Debezium connector.** In
`config/debezium/erp-sqlserver.json`, add the table to `table.include.list`,
comma-separated with no spaces:

```json
"table.include.list": "dbo.Orders,dbo.OrderItems,dbo.Stock,dbo.Payments,dbo.Shipments,dbo.Suppliers",
```

Then apply the change to the connector:

```bash
docker compose --profile cdc up debezium-register
```

It should print `connector 'erp-sqlserver' updated`.

**Step 3. Request a copy of the rows that already exist (snapshot).** Without
this step, only changes made from now on arrive. On SQL Server:

```sql
USE SimulatedErp;
INSERT INTO dbo.DebeziumSignal (id, type, data)
VALUES ('load-Suppliers-1', 'execute-snapshot',
  '{"data-collections": ["SimulatedErp.dbo.Suppliers"],
    "type": "incremental"}');
```

- The `id` must be different for every request.
- In `data-collections`, the name uses the `database.dbo.Table` format.
- Debezium copies the table in blocks of 5,000 rows, without pausing the
  capture of the other tables.
- To follow along: in Kafka UI, the topic `erp.SimulatedErp.dbo.Suppliers`
  keeps receiving messages.

**Step 4. Add it to the CDC Spark job.** Do this only after the topic has
received at least one message (step 3, or any change at the source): the job
waits for every table's schema in the registry before starting, and while it
waits, **no** table is processed. In `docker-compose.yml`, in the `spark-cdc`
service, add the table to `TABLES`:

```yaml
      TABLES: Orders,OrderItems,Stock,Payments,Shipments,Suppliers
```

Then recreate the container:

```bash
docker compose --profile cdc up -d spark-cdc
```

The job discovers the primary key on its own and creates
`bronze.erp_suppliers` and `silver.erp_suppliers`. The log should show
`table lakehouse.silver.erp_suppliers created`:

```bash
docker compose logs --since 5m spark-cdc | grep erp_cdc
```

**Step 5. Check after about 2 minutes (Trino):**

```sql
SELECT count(*) FROM lakehouse.silver.erp_suppliers;

SELECT op, count(*)
FROM lakehouse.bronze.erp_suppliers
GROUP BY op;
```

In bronze, `r` rows come from the snapshot; `c`, `u` and `d` are inserts,
updates and deletes.

> **The Kafka Connect log** shows this warning at every start:
> `Exception caught while calling sys.sp_cdc_help_jobs ... DBO privileges are required`.
> You can ignore it: the CDC login is read-only on purpose, and Debezium falls
> back to default values.

### 2C. Taking the table to gold and to the DW (Tableau)

Silver is an exact copy of the source. For the table to reach Tableau:

1. **Business rule:** write the transformation in a gold job, for example
   `jobs/gold/sales.py` or a new file in `jobs/gold/`, writing to
   `lakehouse.gold.<name>`. Try it first in JupyterLab.
2. **Delivery:** add the gold table name to `DEFAULT_TABLES` in
   `jobs/delivery/gold_to_dw.py`. The table is created in the DW
   automatically on the first delivery.
3. **If it is a new job:** create its task in the `erp_sales_hourly` DAG,
   before `deliver_dw`.
4. **Order matters:** change the gold job only after the new silver table
   exists (sections 2A/2B done and the first load finished). Otherwise the
   next hourly run fails with `TABLE_OR_VIEW_NOT_FOUND`. If that happens,
   finish the load and click **Clear** on the failed task.
5. **In the DW:** business users read through the `dw_reader` role. If the
   table holds sensitive data, give it its own role and permissions.

### 2D. Removing a table from CDC

1. Remove the table from `TABLES` in `docker-compose.yml` and run:
   ```bash
   docker compose --profile cdc up -d spark-cdc
   ```
2. Remove the table from `table.include.list` and run:
   ```bash
   docker compose --profile cdc up debezium-register
   ```
3. The DBA disables CDC on the table:
   ```sql
   EXEC sys.sp_cdc_disable_table
        @source_schema = N'dbo',
        @source_name = N'Suppliers',
        @capture_instance = N'all';
   ```
4. **Optional, to delete the data from the lake:** this cannot be undone.
   - In Trino:
     ```sql
     DROP TABLE lakehouse.bronze.erp_suppliers;
     DROP TABLE lakehouse.silver.erp_suppliers;
     ```
   - Delete the topic:
     ```bash
     docker exec datalake-kafka-1 /opt/kafka/bin/kafka-topics.sh \
       --bootstrap-server kafka:9092 --delete \
       --topic erp.SimulatedErp.dbo.Suppliers
     ```
   - Delete the checkpoint:
     ```bash
     docker exec datalake-spark-cdc-1 \
       rm -rf /checkpoints/cdc/erp_suppliers
     ```

   If you do not delete the checkpoint and the table comes back one day, the
   job resumes where it stopped.

---

## 3. When something goes wrong

| Symptom | What to do |
|---|---|
| Red task in Airflow | Click it → **Logs** and look for the first line with `Error` or `Exception`. Once the cause is fixed, click **Clear** on the task to run it again. Airflow already retries twice on its own before marking it failed |
| Connector `FAILED` in Kafka UI | See the reason in Kafka UI itself (or `curl localhost:8083/connectors/erp-sqlserver/status`). Common causes: SQL Server down, password changed, SQL Server Agent stopped. Once fixed: `curl -X POST localhost:8083/connectors/erp-sqlserver/tasks/0/restart` |
| `erp_cdc` or `bronze_orders` disappeared from Spark | `docker compose logs --since 30m spark-cdc` to see the error, then `docker compose restart spark-cdc`. The job resumes where it stopped (checkpoint), without losing or duplicating silver data |
| The structure of a CDC table changed at the source | `docker compose restart spark-cdc` (the schema is read at startup). On SQL Server, the DBA must recreate the table's CDC capture instance |
| DBeaver cannot connect to Trino | Port **8088** (not 8080). Check that Trino is up: `docker compose ps trino` |
| DW numbers differ from the lake | Delivery runs at the top of every hour. Check the latest run of `deliver_dw` in Airflow |
| PC slow, memory full | `docker stats --no-stream`. Close forgotten Spark sessions in JupyterLab (`spark.stop()` or close the kernel) |
| Disk filling up | `docker system df`. Old images: `docker image prune`. **Never** run `docker compose down -v` by accident: `-v` deletes all the data. Check that `lakehouse_maintenance` is running every night (section 5). Space freed by maintenance only returns to the disk after the SeaweedFS *vacuum*, which runs on its own; to force it: `printf 'lock\nvolume.vacuum -garbageThreshold 0.1\nunlock\n' \| docker exec -i datalake-seaweedfs-1 weed shell` |
| `lakehouse_maintenance` failed | Open the log: each table prints `FAILED:` with the reason. A conflict with the CDC job (`ValidationException`, `Found conflicting files`) is harmless: click **Clear** to run again. The other tables were maintained normally |
| `erp_cdc` stopped for more than 3 days and now fails at startup with an error about a snapshot that was expired or cannot be found | The bronze snapshot its checkpoint points to was expired by maintenance. Rebuild that table's silver from bronze, which keeps the whole history (nothing is lost): 1) `docker compose stop spark-cdc`; 2) in Trino, `DROP TABLE lakehouse.silver.erp_<table>`; 3) `docker compose run --rm --no-deps --entrypoint rm spark-cdc -rf /checkpoints/cdc/erp_<table>/silver`; 4) `docker compose --profile cdc up -d spark-cdc`. The job recreates the table and reapplies every change |

---

## 4. Saving changes (git)

Every change to project files (DAG, connector, compose) should go into git, so
it is recorded and can be undone.

Commits work without a network connection and are stored locally:

```bash
git status                       # what changed
git add airflow/dags/erp_sales.py
git commit -m "Add Suppliers to the hourly load"
```

When the remote is reachable, push everything at once:

```bash
git push
```

To discard an uncommitted change to a file:

```bash
git restore <file>
```

---

## 5. Test data and maintenance

The concepts behind this section (small files, compression, late data) are
explained in [MAINTENANCE.md](MAINTENANCE.md).

### Generating test data

Only with the simulated SQL Server (`simulated-source` profile), never on the
real one.

| To | Command |
|---|---|
| Months of past orders (100,000 orders over 180 days, 20,000 customers, 300 products, with payments and shipments) | `docker compose run --rm erp-history` |
| A different volume | `docker compose run --rm -e ORDERS=500000 -e DAYS=365 -e CUSTOMERS=50000 erp-history` |
| Live ERP activity (3 orders every 5 s; Ctrl+C to stop) | `docker compose run --rm erp-simulator` |
| Kafka events (the `streaming` profile must be up) | `docker compose run --rm order-producer --total 10000` |
| Kafka events with 5% arriving up to 72 h late | `docker compose run --rm order-producer --total 10000 --late-ratio 0.05` |

`erp-history` takes seconds on SQL Server; CDC then needs a few minutes to
bring everything to silver. Customers and products arrive with the next
scheduled load (or trigger `erp_sales_daily`).

To check that silver matches the source after a load, compare in DBeaver:

```sql
-- SQL Server
SELECT COUNT(*), SUM(TotalAmount) FROM dbo.Orders;
-- Trino
SELECT count(*), sum(TotalAmount) FROM lakehouse.silver.erp_orders;
```

### Nightly maintenance

The `lakehouse_maintenance` DAG runs at 3 a.m. with no intervention: it
compacts small files, applies delete files, expires snapshots older than 3
days and removes orphan files in `bronze`, `silver`, `gold` and `ops`.

**Is it working?** In Airflow, the DAG must be green every morning. In
DBeaver, the number of files of a streaming table should drop after each run:

```sql
SELECT count(*) AS files, round(avg(file_size_in_bytes) / 1024) AS avg_kb
FROM lakehouse.bronze."erp_orders$files";
```

**Running it by hand** (for one table, or to see the effect right away):

```bash
docker exec datalake-airflow-scheduler-1 /opt/spark/submit-job.sh \
  /opt/airflow/jobs/maintenance/iceberg_maintenance.py --namespace silver --table erp_orders
```

The log shows the table before and after (files, delete files, snapshots).
`--dry-run` only shows the state and how many orphan files would be deleted.

**Settings** (in `airflow/dags/lakehouse_maintenance.py`):

- `RETAIN_DAYS = 3`: how far back time travel works. It must also be longer
  than the longest the `spark-cdc` job may stay stopped (see section 3).
  If the PC may stay off for a week, raise it to 8.
- A new namespace (for example `sandbox`) only needs to be added to `NAMESPACES`.

### Late events (`orders_events_hourly`)

Every hour, at :15, the DAG updates `gold.orders_daily` (orders and revenue
per event day, channel and status). It recomputes only the days touched by
the events that arrived since the previous run, including late ones from
past days. `late_orders` and `max_delay_hours` show how much arrived late.

To rebuild the whole table (after changing the rule in
`jobs/gold/orders_daily.py`): in Airflow, **Trigger DAG w/ config** and set
`{"full": true}`.

