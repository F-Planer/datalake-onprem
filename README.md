# On-premises Lakehouse

A complete, **100% open-source** data lakehouse that runs on a single machine
with Docker Compose. Streaming, change data capture (CDC) and batch ingestion
land in Apache Iceberg tables organized in bronze, silver and gold layers,
which Spark processes and Trino queries, and a relational data warehouse
serves to BI tools.

A simulated ERP (SQL Server) and an event generator produce realistic data,
so every flow can be run and verified end to end.

**What it demonstrates**

- **Streaming:** Kafka events in Avro (Schema Registry format) → Spark Structured Streaming → Iceberg.
- **CDC:** SQL Server CDC → Debezium → Kafka → Spark: append-only change history in bronze, exact current state in silver (MERGE, deletes included).
- **Batch:** incremental and full JDBC loads orchestrated by Airflow, with lookback windows and reconciliation.
- **Dimensional modeling:** a sales star schema in gold, delivered atomically to a SQL Server data warehouse.
- **Late data:** watermark-driven incremental aggregates that recompute only the partitions late events touch.
- **Table maintenance:** nightly compaction, delete-file cleanup, snapshot expiry and orphan-file removal.
- **Operations:** health check, runbooks for adding and removing tables, and troubleshooting.

**Documentation**

| Document | Contents |
|---|---|
| [`docs/FUNDAMENTALS.md`](docs/FUNDAMENTALS.md) | Architecture fundamentals: columnar storage, Iceberg, Spark, Kafka, CDC, modeling, scalability, AI readiness |
| [`docs/OPERATIONS.md`](docs/OPERATIONS.md) | Operations manual: where to look, adding or removing tables, troubleshooting, test data and maintenance |
| [`docs/MAINTENANCE.md`](docs/MAINTENANCE.md) | Iceberg maintenance, Parquet compression and late-data strategies per layer, with measured results |

---

## Architecture

```
 SQL Server ─(CDC)─► Debezium / Kafka Connect ─► Kafka ─► Spark Structured Streaming ─┐
 (near-real-time tables)                           │                                  │
                                                Karapace                              ▼
 Event producers ─(Avro)─────────────────────────► Kafka          Iceberg tables: bronze ► silver ► gold
                                                                        │             │
 SQL Server ─(JDBC)─► Airflow ─► Spark batch ───────────────────►  SeaweedFS       Polaris
 (other tables: hourly/daily loads)                                (S3 files)     (REST catalog)
                                                                        │             ▲
                                                  gold ─(Airflow)───────┘             │
                                                    │                    Trino (SQL: engineering, exploration)
                                                    ▼
                                      SQL Server data warehouse ──► BI tools
```

**Medallion layers**

| Layer | Contents | Written by |
|---|---|---|
| **bronze** | Raw data as received, append-only, with ingestion metadata (`_kafka_offset`, `_ingested_at`, `_load_id`...) | Streaming, CDC and batch jobs |
| **silver** | Exact current state of each source table (MERGE by primary key) | CDC and batch jobs |
| **gold** | Business-ready star schema and aggregates | Gold jobs (Airflow) |
| **ops** | Pipeline control tables (watermarks) | Incremental jobs |

**Design principles**

- **The lake processes, the warehouse serves.** Business rules live once, in versioned lake jobs. The warehouse only receives the finished gold layer (plus indexes for BI), so numbers never diverge.
- **Idempotent by design.** Delivery is at-least-once everywhere; MERGE by key, deduplication and watermarks saved after commit make every retry safe.
- **Open formats.** Data lives in Parquet + Iceberg behind a standard REST catalog: any compatible engine can read it.
- **Open source only.** Every component is under an OSI-approved license. Source-available licenses (BSL, SSPL, ELv2, Confluent Community License) are excluded, and container images are checked too.

## Stack

| Role | Component | License |
|---|---|---|
| Messaging / streaming | **Apache Kafka** (KRaft, 1 broker) | Apache 2.0 |
| Schema registry | **Karapace** | Apache 2.0 |
| Kafka UI | **Kafka UI (kafbat)** | Apache 2.0 |
| CDC | **Debezium** on Kafka Connect | Apache 2.0 |
| S3 object storage | **SeaweedFS** | Apache 2.0 |
| Table format | **Apache Iceberg** | Apache 2.0 |
| Iceberg catalog | **Apache Polaris** (Iceberg REST) | Apache 2.0 |
| Batch + streaming processing | **Apache Spark** | Apache 2.0 |
| SQL query engine | **Trino** | Apache 2.0 |
| Orchestration | **Apache Airflow** (LocalExecutor) | Apache 2.0 |
| Metadata database | **PostgreSQL** (Polaris + Airflow) | PostgreSQL License |
| Notebooks | **JupyterLab** | BSD-3-Clause |
| Simulated source and warehouse | **SQL Server 2025 Developer** | Proprietary, free for development and testing |

SQL Server is used because CDC from a commercial OLTP database is the common
real-world case. Developer edition is free for non-production use and
includes CDC (Express does not, since it lacks SQL Server Agent).

**Why these choices**

- **Kafka in KRaft mode** instead of Redpanda (BSL); no ZooKeeper.
- **Karapace** instead of Confluent Schema Registry (Confluent Community License): same API, lighter.
- **SeaweedFS** instead of MinIO (license and distribution changes) and Garage (AGPL).
- **Polaris** implements the Iceberg REST catalog standard, which Spark and Trino speak natively, so switching catalogs is cheap (Lakekeeper is a lightweight alternative).
- **Spark for both batch and streaming**: one engine instead of Spark + Flink saves memory and learning time.
- **Official project images** instead of third-party repackaged ones.

**Possible extensions**

| Component | License | Use |
|---|---|---|
| dbt-core + dbt-trino | Apache 2.0 | Gold transformations as tested, documented SQL models (avoid dbt Fusion, ELv2) |
| Apache Superset | Apache 2.0 | Dashboards directly on Trino |
| Ceph (RGW) instead of SeaweedFS | LGPL | Multi-node storage with high availability (see below) |

---

## Running it

**Requirements:** Linux with Docker Engine and Docker Compose v2. With every
profile up the stack uses about 8–10 GiB of RAM; 16 GB is the minimum and
32 GB is comfortable. Java is not needed on the host (it ships in the images).
`scripts/01-install-docker.sh` installs Docker Engine on Ubuntu.

```bash
cp .env.example .env && chmod 600 .env     # then replace every change-this-password
                                           # (e.g. with: openssl rand -base64 24)

docker compose up -d                                      # base: Postgres, SeaweedFS, Polaris + init jobs
docker compose --profile streaming up -d                  # Kafka, Karapace, Kafka UI, Spark, streaming job
docker compose --profile simulated-source up -d           # simulated SQL Server (ERP with CDC + DW)
docker compose --profile cdc up -d                        # Debezium + CDC job (bronze and silver)
docker compose --profile batch up -d                      # Airflow (loads, gold, DW delivery, maintenance)
docker compose --profile query up -d                      # Trino
docker compose --profile notebook up -d                   # JupyterLab connected to Spark and the lakehouse

docker compose run --rm order-producer --total 1000       # publish 1,000 test events
docker compose run --rm erp-history                       # 100,000 past orders in the ERP
docker compose run --rm erp-simulator                     # live ERP activity (Ctrl+C to stop)

./scripts/check.sh                                        # health check
docker compose down                                       # stop, keeping data
docker compose down -v                                    # stop and DELETE all data
```

Init jobs (`*-bucket`, `*-bootstrap`, `*-catalog`, `sqlserver-init`) are
idempotent and must exit with code 0 (`docker compose ps -a`).

**Web UIs and endpoints** (bound to `127.0.0.1` only):

| Service | Address |
|---|---|
| Airflow (user `admin`, password `AIRFLOW_ADMIN_PASSWORD`) | http://localhost:8095 |
| Trino (web UI; SQL clients such as DBeaver) | http://localhost:8088 |
| JupyterLab (token `JUPYTER_TOKEN`) | http://localhost:8899 |
| Kafka UI | http://localhost:8085 |
| Spark master / worker / streaming job / CDC job | http://localhost:8090 / :8091 / :4040 / :4041 |
| Kafka Connect REST API | http://localhost:8083 |
| Karapace (Schema Registry) | http://localhost:8081 |
| Polaris (Iceberg REST at `/api/catalog`) | http://localhost:8181 |
| SeaweedFS: S3 / file browser / master | `:8333` / http://localhost:8888 / http://localhost:9333 |
| Kafka (host clients) | `localhost:29092` |
| SQL Server / Postgres | `localhost,1433` / `localhost:5432` |

**Pinned versions:** Postgres 18, SeaweedFS 4.54, Polaris 1.9.0, Kafka 4.4.0,
Karapace 6.2.4, Kafka UI 1.5.0, Spark 4.1.4, Iceberg 1.12.0, Debezium 3.7.1,
SQL Server 2025 CU11, Airflow 3.3.2, Trino 484. Spark stays on 4.1 because
Iceberg 1.12 ships a runtime up to Spark 4.1.

## Repository layout

| Path | Contents |
|---|---|
| `docker-compose.yml` | All services; the base runs without a profile, the rest belong to `streaming`, `cdc`, `simulated-source`, `generator`, `batch`, `query` and `notebook` |
| `.env.example` | Template for the secrets file `.env` (never committed) |
| `config/postgres/`, `config/seaweedfs/`, `config/polaris/` | Database creation, S3 credentials and bucket, Polaris realm and `lakehouse` catalog |
| `config/sqlserver/` | Simulated ERP (`SimulatedErp`, 8 tables), activity simulator, history generator, CDC and least-privilege logins, DW database |
| `config/debezium/` | One JSON per connector; `register.sh` creates or updates them |
| `config/trino/` | Trino configuration and the `lakehouse` catalog |
| `spark/` | Spark image (Iceberg, Kafka and Avro jars verified by checksum), defaults and `submit-job.sh` (injects secrets) |
| `kafka-connect/` | Kafka Connect image: Debezium + Avro converter |
| `producers/orders/` | Avro event generator; `--late-ratio` sends part of the events late |
| `jobs/streaming/bronze_orders.py` | Kafka `orders` topic → `bronze.orders` every 30 s |
| `jobs/cdc/erp_cdc.py` | Debezium topics → `bronze.erp_<table>` (history) → `silver.erp_<table>` (MERGE) |
| `jobs/batch/erp_load.py` | JDBC load, incremental or full → `bronze.erp_<table>` → `silver.erp_<table>` |
| `jobs/gold/sales.py` | Star schema: `dim_product`, `dim_customer`, `fact_sales_items`, `daily_sales`, `fact_payments`, `fact_shipments` |
| `jobs/gold/orders_daily.py` | Incremental aggregate of the Kafka events with late-data handling |
| `jobs/delivery/gold_to_dw.py` | Gold → SQL Server DW (staging + one-transaction swap, columnstore) |
| `jobs/maintenance/iceberg_maintenance.py` | Iceberg maintenance per namespace, with before/after metrics |
| `airflow/` | Airflow image and DAGs: `erp_sales_hourly`, `erp_sales_daily`, `orders_events_hourly`, `lakehouse_maintenance` |
| `notebook/`, `notebooks/` | JupyterLab image and a getting-started notebook (read, transform, write, Trino, time travel) |
| `queries/` | Example SQL for Trino (events, change history, rebuilding state from bronze) |
| `scripts/` | `check.sh` (health check), Docker install and clock-sync helpers |

---

## How the flows work

**Streaming**

- Messages use the Schema Registry wire format (magic byte + schema id + Avro). The job fetches the schema from Karapace and decodes with Spark's `from_avro`, without Confluent libraries.
- Bronze stores the Kafka origin (`_kafka_partition`, `_kafka_offset`, `_kafka_timestamp`), `_schema_id` and `_ingested_at`, and is partitioned by **ingestion** day, so late events never touch old partitions.
- Progress lives in the checkpoint; restarting the job resumes without loss or duplicates.

**CDC**

- SQL Server CDC → Debezium → one Avro topic per table → `spark-cdc` job with two queries per table:
  - **bronze** `erp_<table>`: one row per change (`op` r/c/u/d, `before`, `after`, LSN, commit time), every 30 s;
  - **silver** `erp_<table>`: current state via MERGE every minute; deletes at the source become deletes.
- Primary keys are discovered from the key schema Debezium registers, so adding a table needs no code.
- New tables get their existing rows through an **incremental snapshot** requested with an INSERT into a signal table, without pausing the other tables.
- At-least-once delivery: bronze may hold repeated events; silver is unaffected because it applies the latest event per key in Kafka order.
- Credentials never live in Kafka: connectors read them from the environment (`${env:...}`).
- Least privilege on the source: separate read-only logins for CDC and for batch loads, each seeing only its own tables.

**Batch and delivery**

- **Incremental load:** rows with `UpdatedAt` ≥ last loaded mark minus a lookback window (late-committing transactions), applied with MERGE and a guard that never lets an older version overwrite a newer one. Deletes are caught by the **daily full load**, which doubles as reconciliation.
- **Gold:** rebuilt on every run with an atomic Iceberg swap. Personal data (e-mail) stays out of gold.
- **DW delivery:** writes `stg.<table>`, then empties and reloads `dbo.<table>` in one transaction, so readers never see half-loaded data. Tables are created automatically with a columnstore index.
- **Airflow:** each task runs `spark-submit` against the cluster; a pool caps concurrent Spark jobs.

| DAG | Schedule | Does |
|---|---|---|
| `erp_sales_hourly` | every hour | incremental loads → gold → DW delivery |
| `erp_sales_daily` | daily, 1 a.m. | full loads (small tables + reconciliation) |
| `orders_events_hourly` | every hour, :15 | `gold.orders_daily`, recomputing only the event days touched by new or late events |
| `lakehouse_maintenance` | daily, 3 a.m. | compaction, delete files, manifests, snapshot expiry, orphan files |

**Catalog and storage**

- SeaweedFS has no STS, so Polaris does not vend credentials (`stsUnavailable: true`): each client uses its own S3 key with path-style access. Clients must not request credential vending.
- Trino reads all three layers, including merge-on-read tables, snapshot history and time travel.

---

## Validation

Every flow was verified by comparing the source with the lake:

- **Streaming:** 122,000 published events reached bronze with no duplicates, including after job restarts.
- **CDC:** with ~1 million rows in the lake, silver matched the source exactly (row count and sums) in all 5 CDC tables, including after restarting Debezium and Spark in the middle of the activity.
- **End to end:** ERP, gold and DW returned the same orders, items and revenue.
- **Late data:** 3,000 events up to 48 h late made the incremental job rewrite only the 3 affected day partitions; the untouched day kept its computation timestamp, and totals had no duplicates.
- **Maintenance:** while the simulator kept writing, streaming tables went from ~65 small files to 1–2 and silver tables from ~35 data + ~30 delete files to a single file, with no conflict and no change to the data.

Measured numbers are in [`docs/MAINTENANCE.md`](docs/MAINTENANCE.md#6-results-in-this-poc-2026-11-22).

## Limitations

This is a single-node reference implementation, not a production deployment:

- one broker, one Spark worker and one storage node: no replication or high availability;
- services use root/admin credentials and Trino has no authentication (all ports bound to `127.0.0.1`);
- no TLS between services, no centralized identity, no metrics/alerting stack.

The natural next steps for production are per-service principals in Polaris
and S3, Trino with HTTPS and LDAP/OIDC, Prometheus and Grafana, and a
multi-node deployment (for example Kubernetes, with Ceph or another
replicated object store).

### Replacing SeaweedFS with Ceph

Every client speaks plain S3, and Ceph exposes the same API through **RGW**.
On a single machine Ceph is a poor fit (several daemons, ~5–7 GiB of RAM, a
dedicated disk for the OSD, and no real benefit without replicas), but on 3+
nodes it brings high availability and STS (credential vending in Polaris).
Migration:

1. Create an S3 user and a bucket with the same name (`lakehouse`) in RGW.
2. Copy the objects (e.g. `rclone sync`). Iceberg metadata stores `s3://lakehouse/...` paths without the endpoint, so nothing is rewritten.
3. Point `config/polaris/create-catalog.sh`, `config/trino/catalog/lakehouse.properties` and `spark/conf/spark-defaults.conf` to the RGW endpoint, update the S3 keys in `.env`, and update the catalog's storage endpoint.
4. Remove the `seaweedfs` services from `docker-compose.yml`.
5. Optionally enable STS and credential vending.
