# Data architecture fundamentals

> A study guide built from this POC. It explains **why** each piece exists, how the pieces talk to each other and how they scale, and what a mid-level or senior data engineer needs to master to design efficient solutions.
>
> Installation and architecture are in [`README.md`](../README.md); day-to-day operation is in [`OPERATIONS.md`](OPERATIONS.md).

## Contents

1. [The problem this architecture solves](#1-the-problem-this-architecture-solves)
2. [The layered topology](#2-the-layered-topology)
3. [Storage: why columnar is so fast](#3-storage-why-columnar-is-so-fast)
4. [Table format: what Iceberg adds to Parquet](#4-table-format-what-iceberg-adds-to-parquet)
5. [Catalog and the separation of compute and storage](#5-catalog-and-the-separation-of-compute-and-storage)
6. [Distributed processing: Spark, Trino and DuckDB](#6-distributed-processing-spark-trino-and-duckdb)
7. [Streaming and CDC](#7-streaming-and-cdc)
8. [Batch and orchestration](#8-batch-and-orchestration)
9. [Modeling: medallion and dimensional](#9-modeling-medallion-and-dimensional)
10. [Quality, observability and data contracts](#10-quality-observability-and-data-contracts)
11. [Security and governance](#11-security-and-governance)
12. [Scalability: how each piece grows and communicates](#12-scalability-how-each-piece-grows-and-communicates)
13. [From data mart to AI](#13-from-data-mart-to-ai)
14. [What is expected from a mid-level and a senior engineer](#14-what-is-expected-from-a-mid-level-and-a-senior-engineer)
15. [Common anti-patterns](#15-common-anti-patterns)
16. [Study path](#16-study-path)

---

## 1. The problem this architecture solves

Every company has data, almost always locked inside the database of the system that created it (ERP, CRM, spreadsheets). That database is **OLTP** (*Online Transaction Processing*): built for many small, concurrent operations, such as writing an order or reading a customer by key. It is bad at the typical analytical question, which is **OLAP** (*Online Analytical Processing*): "what is the revenue by category and state over the last 3 years?", which reads millions of rows and few columns.

| | OLTP (ERP) | OLAP (lake / DW) |
|---|---|---|
| Typical operation | Write and read a few rows by key | Read millions of rows, aggregate |
| Physical layout | Row-oriented | Column-oriented |
| Model | Normalized (avoids redundancy) | Dimensional or denormalized (avoids joins) |
| History | Current state | Current state **and** history |
| Priority | Integrity and per-transaction latency | Read throughput |

Running heavy analytics on the ERP database competes with the company's operations and hits the wrong layout. A data architecture exists to:

1. **Copy** data from the source without disturbing it (CDC, incremental loads).
2. **Store** everything, with history, on cheap storage and in a format that is efficient to read.
3. **Transform** raw data into trustworthy data, with business rules versioned as code.
4. **Serve** the result to its consumers: BI, analysts, applications and AI models.

The "data revolution" companies are after is rarely blocked by a lack of technology. It is usually blocked by the lack of **a single source of truth**: each department computes "revenue" its own way, nobody knows where a number came from, and the report does not match the system. The architecture solves the technical part; the part that creates value is **discipline**: business rules in one place, tests, documentation and reconciliation with the source.

---

## 2. The layered topology

A modern data platform is a set of **responsibilities**. Different tools can fulfill each one; what matters is understanding the role.

```
        ┌──────────────────────────────────────────────────────────────────────┐
        │                      ORCHESTRATION (Airflow)                         │
        │        when it runs, in what order, what to do if it fails           │
        └──────────────────────────────────────────────────────────────────────┘
 ┌─────────┐   ┌──────────────┐   ┌──────────────────────────────────┐   ┌─────────────┐
 │ SOURCES │──►│  INGESTION   │──►│      PROCESSING (Spark)          │──►│  CONSUMPTION│
 │ ERP SQL │   │ Debezium     │   │  bronze ─► silver ─► gold        │   │ Trino       │
 │ Server  │   │ Kafka        │   │                                  │   │ SQL Server  │
 │         │   │ JDBC (batch) │   └───────────────┬──────────────────┘   │ DW ► Tableau│
 └─────────┘   └──────────────┘                   │ reads and writes     │ AI / ML     │
                                                  ▼                      └─────────────┘
        ┌──────────────────────────────────────────────────────────────────────┐
        │  CATALOG (Polaris)  — which tables exist and their current version   │
        ├──────────────────────────────────────────────────────────────────────┤
        │  TABLE FORMAT (Iceberg) — transactions, history, schema              │
        ├──────────────────────────────────────────────────────────────────────┤
        │  FILE FORMAT (Parquet) — columnar, compressed                        │
        ├──────────────────────────────────────────────────────────────────────┤
        │  OBJECT STORAGE (SeaweedFS, S3 API) — cheap bytes                    │
        └──────────────────────────────────────────────────────────────────────┘
        Cross-cutting: security, quality, observability, lineage, metadata
```

| Responsibility | Question it answers | In this POC | Market alternatives |
|---|---|---|---|
| Source | Where is the data born? | SQL Server (ERP) | Any OLTP, APIs, files, events |
| Ingestion | How does data leave the source? | Debezium + Kafka (CDC), JDBC (batch) | Fivetran, Airbyte, Kafka Connect, NiFi |
| Storage | Where do the bytes live? | SeaweedFS (S3) | AWS S3, Azure ADLS, GCS, Ceph, MinIO |
| File format | How are the bytes organized? | Parquet | ORC, Avro (row-based) |
| Table format | How do files become a reliable table? | Iceberg | Delta Lake, Apache Hudi |
| Catalog | Which tables exist, where, and who can access them? | Polaris | Unity Catalog, Glue, Nessie, Lakekeeper |
| Processing | Who transforms the data? | Spark | Flink, dbt (SQL), Trino, DuckDB |
| Orchestration | When and in what order? | Airflow | Dagster, Prefect |
| Consumption | How do people use it? | Trino, SQL Server DW, Tableau | Superset, Power BI, APIs |

This architecture is called a **lakehouse**: the cheap, open storage of a *data lake* with the guarantees of a *data warehouse* (transactions, schema, SQL). The key word is **open**: data lives in public formats (Parquet + Iceberg), and any compatible engine can read and write it. No vendor owns your data.

### Control plane and data plane

A distinction that helps you understand any distributed system:

- **Control plane:** coordinates and keeps metadata. Little traffic, but it must always be up. Here: Polaris, Spark master, Airflow scheduler, Trino coordinator, Kafka controller.
- **Data plane:** moves and processes the bytes. Heavy traffic, scales horizontally. Here: SeaweedFS, Spark executors, Trino workers, Kafka brokers.

To scale, you multiply the data plane. For high availability, you replicate the control plane.

---

## 3. Storage: why columnar is so fast

If you have ever compared Parquet + DuckDB with a traditional SQL database and seen a big difference in response time, this is why.

### Rows versus columns

```
Row-oriented (rowstore, ERP):         Column-oriented (Parquet):
[1, Ana,  SP, 100.0]                  id:     [1, 2, 3, 4]
[2, Bruno,RJ,  50.0]                  name:   [Ana, Bruno, Caio, Dani]
[3, Caio, SP,  70.0]                  state:  [SP, RJ, SP, SP]
[4, Dani, SP,  20.0]                  amount: [100.0, 50.0, 70.0, 20.0]
```

`SELECT state, SUM(amount) ... GROUP BY state` on a row layout reads **every** column of every row. On a columnar layout, it reads only `state` and `amount`. On an 80-column table, that is already 40 times less I/O.

### What is inside a Parquet file

```
file.parquet
 ├── Row group 1  (~128 MB of rows)
 │    ├── column id     → pages + statistics (min, max, nulls)
 │    ├── column state  → dictionary {SP, RJ} + compressed indexes
 │    └── column amount → compressed pages
 ├── Row group 2 ...
 └── Footer: schema + statistics for each row group and column
```

Four techniques add up:

1. **Projection pushdown:** reads only the requested columns.
2. **Predicate pushdown:** `WHERE date = '2026-11-18'` checks the min/max of each row group in the footer and skips the ones that cannot contain that date, without reading them.
3. **Encoding:** repetitive columns become dictionary + RLE (*run-length encoding*): "SP, SP, SP, SP" becomes "SP ×4". A state column with millions of rows takes almost no space.
4. **Compression** (Snappy, Zstd) on top, which works much better because the values in a column are similar to each other.

And on the engine side:

5. **Vectorized execution:** DuckDB, Trino and Spark process blocks of thousands of values from one column at a time, making good use of CPU caches and SIMD instructions, instead of interpreting row by row.

SQL Server also has a columnar layout: the **columnstore** index, used in this POC's DW. The gain you measured is not "lake versus database", it is **columnar versus row**. Choosing between a lake and a database is about cost, scale, open formats and separating compute from storage.

### Object storage is not a disk

S3 (and SeaweedFS, which implements its API) stores **immutable objects** accessed over HTTP:

- There is no "change 10 bytes in the middle of a file": you write a whole new object.
- There is no atomic directory *rename* (which is why classic Hadoop struggled on S3).
- Each request has a latency of milliseconds, so **reading a thousand small files is much worse than reading one big file**.
- In return: practically unlimited capacity, cheap, durable and reachable from any machine on the network.

The practical consequence: **small files are a lake's number one enemy**. Streaming that writes every 30 s produces many small files, which is why **compaction** exists (section 4).

---

## 4. Table format: what Iceberg adds to Parquet

A folder full of Parquet files is not a table. You still need to know: which files belong to the current version? What happens if a job fails halfway through a write? How do you delete a row? How do you rename a column without rewriting everything?

Iceberg answers with a **metadata tree**:

```
Catalog (Polaris): "silver.erp_orders → metadata v42"
                     │
                     ▼
     v42.metadata.json  ── schema, partitions, list of snapshots
                     │
                     ▼
     current snapshot → manifest list (list of manifests)
                     │
          ┌──────────┴──────────┐
          ▼                     ▼
     manifest A             manifest B   ── each one lists data files
          │                     │           with per-column statistics
          ▼                     ▼
     a.parquet  b.parquet   c.parquet  d-deletes.parquet
```

### How a write becomes an ACID transaction

1. The job writes the **new files** to S3. Nobody can see them yet.
2. It writes the new manifests and a new `metadata.json` (v43).
3. It asks the catalog: "move the pointer from v42 to v43, **if it is still at v42**". This swap is the only atomic operation in the whole write.
4. If another job has already moved it to v43, the swap fails: Iceberg re-reads, checks whether there is a real conflict and tries again (**optimistic concurrency**).

If the job dies before step 3, the table stays at v42, intact. Orphan files are left behind, which maintenance cleans up later.

### Features you need to know

| Feature | What it is | Where it shows up in this POC |
|---|---|---|
| **Snapshots and *time travel*** | Every commit is a queryable version: `FOR TIMESTAMP AS OF` | Trino queries older versions of silver |
| **Schema evolution** | Columns have internal IDs; renaming and adding do not rewrite data | DDL changes at the source |
| **Hidden partitioning** | Partitioning defined by a transform (`day(_ingested_at)`); users filter on the normal column and Iceberg prunes partitions | Bronze partitioned by day |
| **Copy-on-write (COW)** | UPDATE/DELETE rewrites the affected files. Fast reads, expensive writes | Gold rebuilt in full |
| **Merge-on-read (MOR)** | UPDATE/DELETE writes delete files; reads merge both. Fast writes, reads degrade over time | Silver with MERGE every minute |

### Maintenance (mandatory in production)

An Iceberg table without maintenance gets slow and grows forever:

- **Compaction** (`rewrite_data_files`): merges small files and applies the MOR delete files.
- **Expire snapshots** (`expire_snapshots`): removes old versions and frees the files only they used. Defines how far back *time travel* goes.
- **Remove orphan files** (`remove_orphan_files`): cleans up files from failed writes.
- **Rewrite manifests** (`rewrite_manifests`): keeps query planning fast.

In this project these routines run every night (DAG `lakehouse_maintenance`). How each one works, its trade-offs and the before/after numbers measured in the POC are in [MAINTENANCE.md](MAINTENANCE.md).

Iceberg, Delta Lake and Hudi solve the same problem with similar designs. Iceberg stands out for being neutral (Apache), for its open REST catalog specification and for broad engine support.

---

## 5. Catalog and the separation of compute and storage

The **catalog** is the registry office for tables: it stores each table's name and the pointer to its current metadata, and decides who may read or write. Without it, each engine would have to guess where the files are and there would be no atomic commit.

The **Iceberg REST** protocol standardized that conversation. Spark, Trino, DuckDB, PyIceberg, Flink and Snowflake speak the same protocol. That is why, in this POC, Spark writes silver and Trino reads it, with no copy and no special integration.

### Separating compute from storage

In a traditional database, CPU, memory and disk live in the same box: to get more disk, you buy more CPU, and vice versa. In a lakehouse:

```
   Spark (ETL)      Trino (SQL)      DuckDB (notebook)     AI / Python
        │                │                  │                   │
        └────────────────┴───── catalog ────┴───────────────────┘
                                    │
                           object storage (S3)
```

- Each engine scales **independently**, and can even be turned off when not in use.
- The data exists **only once**; there is no copy per tool.
- You choose the right engine for each workload.
- The cost is the **network**: every byte read crosses it. That is why columnar formats, partition pruning and statistics matter so much.

---

## 6. Distributed processing: Spark, Trino and DuckDB

### How Spark runs a job

```
 Driver (plans)                           Executors (do the work)
 ┌──────────────────────┐                 ┌───────────┐ ┌───────────┐
 │ code → logical plan  │   sends tasks   │ task 1    │ │ task 3    │
 │ → optimizer          │ ──────────────► │ task 2    │ │ task 4    │
 │ → stages and tasks   │ ◄────────────── │ (partition)│ │(partition)│
 └──────────────────────┘     status      └───────────┘ └───────────┘
```

Concepts that come up in every interview and every production issue:

- **Lazy evaluation:** `filter`, `join` and `select` only build a plan. Nothing runs until an **action** (`write`, `count`, `collect`). The optimizer (Catalyst) reorganizes the whole plan before running it.
- **Partition:** the unit of parallelism. One task processes one partition on one core. Too few partitions waste cores; too many add overhead.
- **Narrow versus wide transformation:**
  - *Narrow* (`filter`, `select`): each partition is processed on its own, without the network.
  - *Wide* (`groupBy`, `join`, `distinct`): requires a **shuffle**, which redistributes data across executors by key. It writes to local disk and crosses the network.
- **Stage:** the stretch between two shuffles. The Spark UI (port 4040) shows the stages and is the first place to look when a job is slow.
- **Shuffle** is almost always the dominant cost. Tuning Spark is largely about reducing shuffles.
- **Broadcast join:** when one side of a join is small (a dimension), it is copied to every executor and the large table does not need to be shuffled.
- **Skew:** if 40% of the orders belong to one customer, the task for that key takes much longer than the others and the whole job waits for it. **AQE** (*Adaptive Query Execution*, on by default) splits skewed partitions and adjusts the number of partitions at runtime.
- **Spill:** if a partition does not fit in executor memory, Spark writes to disk and carries on, more slowly. High spill points to too little memory or partitions that are too large.
- **The driver is not for data:** `collect()` and `toPandas()` pull everything into driver memory. On a large table, they bring the job down.

### Trino: in-memory MPP

Trino is an **MPP** (*Massively Parallel Processing*) engine: a coordinator plans, and the workers execute in a **pipeline**, passing data from one operator to the next in memory, without writing intermediate stages to disk. That gives answers in seconds for interactive queries.

The downside: if a query does not fit in the configured memory, it **fails** (in this POC, 1.5 GB per query). For long-running queries, Trino has *fault-tolerant execution*, which writes intermediate results and can retry tasks, but its sweet spot is the interactive query.

### DuckDB: one process, very efficient

DuckDB is an analytical database that runs **inside your process** (Python, CLI), with no server, columnar and vectorized. On a modern machine it handles tens or hundreds of GB comfortably. It proved a point the industry took years to admit: **most analytical workloads fit on a single machine**, and distribution has a cost (network, coordination, complexity).

### Which one to use

| Situation | Engine |
|---|---|
| Heavy ETL, large joins, reprocessing history | Spark |
| Streaming (Kafka → Iceberg) | Spark Structured Streaming (or Flink) |
| Shared, interactive SQL for many people, with access control | Trino |
| Individual exploration, prototyping, tests, data that fits on one machine | DuckDB |
| Simple to medium SQL transformations maintained by analysts | dbt on Trino or Spark |

**Rule:** each table has **one** engine that owns its writes, and each business rule is written **once**. Spark and Trino do not translate each other's SQL: they are independent engines that share the same files.

---

## 7. Streaming and CDC

### Kafka: a distributed log

Kafka is not a queue that deletes messages once read. It is a **log**: a sequence of events, append-only, kept for a period of time (retention).

```
Topic erp.SimulatedErp.dbo.Orders
 Partition 0: [e0][e1][e2][e3][e4] ...   ← consumer A is at offset 3
 Partition 1: [e0][e1][e2] ...           ← consumer B is at offset 1
 Partition 2: [e0][e1][e2][e3] ...
```

- **Partition:** the unit of parallelism and of **ordering**. Order is only guaranteed within a partition. That is why the message key (e.g. `order_id`) defines the partition: all events for the same order stay in order.
- **Offset:** the read position. The consumer keeps track of it. Reprocessing is just moving the offset back.
- **Consumer group:** partitions are shared among the group's members. Maximum read parallelism equals the number of partitions.
- **Decoupling:** the producer does not know who consumes. You can plug in a new consumer (another system, an AI model) without touching the source.

### CDC: capturing changes without querying the table

| Approach | How | Problems |
|---|---|---|
| **Query-based** | `WHERE UpdatedAt > last_mark` | Misses deletes; depends on the date column; adds load to the source |
| **Log-based** | Reads the database transaction log (Debezium) | More moving parts; needs permissions and log retention |

Log-based CDC delivers **every** insert, update and delete, in commit order, with the before and after image of each row. In this POC both approaches coexist: CDC for the near-real-time tables and incremental loads for the rest, with a daily full load to reconcile deletes.

### Schema contracts

The **Schema Registry** (Karapace) stores the Avro schema of each topic and enforces **compatibility** rules: a producer cannot publish a change that would break consumers. It is the first concrete form of a **data contract** between producers and consumers.

### Delivery guarantees

| Guarantee | Meaning | How to deal with it |
|---|---|---|
| At most once | May lose data | Unacceptable for business data |
| **At least once** | Never loses, may repeat | The industry default. The destination must be **idempotent** |
| Exactly once | Neither loses nor repeats | Possible within specific boundaries (Kafka transactions, checkpoint + Iceberg commit), expensive and full of conditions |

In practice, the senior pattern is: **accept at-least-once and make the destination idempotent.** This POC's silver layer does exactly that: it applies the latest event per key with MERGE, so reprocessing never duplicates anything.

### Streaming concepts

- **Micro-batch:** Spark groups the events that arrived in the last N seconds and processes them as a small batch. Latency of seconds, with the batch programming model.
- **Checkpoint:** where the job records which offset it has processed up to. Losing the checkpoint means re-reading everything.
- **Event time versus processing time:** the order happened at 10:00 but arrived at 10:07. Time-window aggregations use event time, and the **watermark** defines how much lateness the job tolerates before closing a window. The batch alternative, used in this POC, is recomputing only the days that late data touched: see [MAINTENANCE.md](MAINTENANCE.md#4-late-data).
- **Real time has a cost:** more moving parts, more small files, more operations. Always ask: "which business decision changes if the data arrives in 1 minute instead of 1 hour?"

---

## 8. Batch and orchestration

### Principles of a good batch job

- **Idempotent:** running twice gives the same result as running once. Use MERGE or overwrite the whole partition; never a blind `INSERT`.
- **Incremental with a lookback window:** this POC's incremental load goes back 10 minutes before the latest mark to catch transactions that committed late. Taking a bit more and deduplicating is safe; taking less loses data silently.
- **Reconciliation:** date-based incremental loads do not see deletes. A periodic full load, or comparing counts and sums, closes the gap.
- **Reprocessable (*backfill*):** the job takes the date range as a parameter, so you can redo a period without hacks.
- **Atomic for consumers:** write to a staging area and swap at the end. Nobody should see half-loaded data (DW delivery does this with `stg` → `dbo`).

### The orchestrator's role

Airflow decides **when** and **in what order**, retries what failed, keeps the history and alerts when something is late. It **should not process data**: tasks send the work to Spark or Trino. An orchestrator overloaded with pandas and large files is a classic anti-pattern.

Concepts: **DAG** (dependency graph), **retry** with delay, **pool** (concurrency limit; here, at most 2 Spark jobs), **SLA/freshness** (the data must be ready by a given time), **sensors** (wait for an event or a file).

---

## 9. Modeling: medallion and dimensional

### Medallion: the path of the data

| Layer | Contents | Main rule | Users |
|---|---|---|---|
| **Bronze** | Data as it arrived, with source metadata (offset, LSN, ingestion time) | Never changed, append only. Allows reprocessing everything | Engineering |
| **Silver** | Clean, typed, deduplicated, current state per key | One row per entity, standardized names and types | Engineering, advanced analysts |
| **Gold** | Modeled for the business: facts, dimensions, aggregates | Business rules applied and tested | BI, business areas, AI |

Bronze is the insurance policy: if the silver or gold logic was wrong, you fix the code and recompute from bronze, without asking the source for the data again.

### Dimensional model (Kimball)

This POC's gold layer follows the **star schema**, the industry standard for BI:

```
              dim_product
                   │
  dim_customer ── fact_sales_items ── dim_date (not in the POC yet; common in production)
                   │
              daily_sales (aggregate)
```

- **Fact:** measurable events (items sold, amount, quantity). Long, narrow tables.
- **Dimension:** the context to filter and group by (customer, product, date). Short, wide tables.
- **Grain:** the most important decision in the model. "One row per order item" is a grain. Mixing grains in one table produces wrong sums.
- **SCD** (*Slowly Changing Dimension*): how to keep changes in a dimension.
  - Type 1: overwrite (the customer moved to another state, and the past "moves" too).
  - Type 2: a new row with validity dates (`valid_from`, `valid_to`). Old sales stay linked to the old state.
- **Surrogate keys:** the dimension has its own key, independent from the ERP key, which enables SCD type 2 and combining several sources.

Other approaches: **OBT** (*One Big Table*, everything denormalized into one wide table, common with columnar engines) and **Data Vault** (for integrating many sources with full auditability). Knowing when each one makes sense is a senior skill.

### Where business rules live

In **one place only**, versioned in git, tested and reviewed. In this POC, in the lake's job code. The DW receives the finished result and recomputes nothing. Two implementations of the same rule (one in Spark, another in a Tableau view) **always** end up diverging.

---

## 10. Quality, observability and data contracts

Wrong data delivered with confidence is worse than no data. A mature engineer treats quality as part of the pipeline, not as a later step.

### What to test

| Type | Example |
|---|---|
| Schema | Expected columns and types |
| Uniqueness | One row per `order_id` in silver |
| Not null | `customer_id` always filled in the fact table |
| Referential integrity | Every `product_id` in the fact exists in `dim_product` |
| Domain | `status` only takes known values |
| Volume | Did 0 orders arrive today? Or 10 times the usual? |
| Freshness | Is the latest data less than 2 hours old? |
| **Reconciliation with the source** | Same counts and sums in the ERP, the lake and the DW |

Reconciliation is the test that gives the project credibility. The POC did it: **346 orders, 1,793 items and BRL 1,533,082.84 matching in the ERP, the gold layer and the DW.** That kind of evidence is what convinces leadership.

Common tools: dbt tests, Great Expectations, Soda, or asserts inside the jobs.

### Observability

Knowing **before the user** that something broke: job duration and failures, Kafka consumer lag, table freshness, volume, disk space. In this POC, `scripts/check.sh` is the seed of that.

### Lineage and metadata

Where did this column come from? Who is affected if I change this table? Tools such as OpenLineage, DataHub and OpenMetadata answer that. In environments with many tables, without lineage nobody dares to change anything.

---

## 11. Security and governance

- **Least privilege:** each service with its own account and only the permissions it needs. In this POC, the CDC login cannot read `Customers` and the batch login cannot read `Orders`.
- **Secrets out of the code:** `.env` with restricted permissions, environment variables; in production, a vault (Vault, a secrets manager).
- **Personal data (LGPD, Brazil's GDPR equivalent):** minimize (`dim_customer` has no e-mail), mask, control access per column and per row, audit who queried what. Personal data in gold needs a justification.
- **Access control at the point of consumption:** business users access the served layer (DW, Trino with login), never the storage or Kafka directly.
- **Governance** is not bureaucracy: it is a defined owner per table, a glossary of terms ("what is revenue?"), sensitivity classification and a process for changes.

---

## 12. Scalability: how each piece grows and communicates

### Vertical before horizontal

- **Vertical (*scale up*):** a bigger machine. Simple, no architectural change, but it has a ceiling and is a single point of failure.
- **Horizontal (*scale out*):** more machines. No practical ceiling, but it brings network, coordination and partial failures.

A modern machine with dozens of cores and hundreds of GB of RAM solves more than it seems. **Distribute when there is a measured reason**, not in advance. This POC runs on a single server with headroom.

### How each component scales

| Component | Unit of scale | How it grows | What limits it |
|---|---|---|---|
| **Kafka** | Partition and broker | More partitions = more parallelism; more brokers = more throughput and replicas (factor 3 in production) | Partition count cannot be reduced later; ordering only exists within a partition |
| **Debezium / Kafka Connect** | Worker and connector | Workers in a cluster share the connectors | One connector reads a database log sequentially |
| **S3 storage** | Storage node | More nodes and disks; replication or *erasure coding* | Small files and too many requests |
| **Catalog (Polaris)** | Stateless replica | Several replicas behind a load balancer; state lives in Postgres | Postgres (primary/replica replication) |
| **Spark** | Executor | More executors on more workers; *dynamic allocation* requests and releases executors with the load | Shuffle (network), skew, driver memory |
| **Trino** | Worker | More workers; queries parallelize automatically | Memory per query; single coordinator |
| **Airflow** | Executor | LocalExecutor (one machine) → CeleryExecutor or KubernetesExecutor (many) | Scheduler and metadata database |
| **DW (SQL Server)** | Machine | Vertical; read replicas | Licensing and hardware |

### Who talks to whom

```
                         ┌──────────── Kubernetes or hosts (where the processes run) ─────────────┐
                         │                                                                        │
 SQL Server ──CDC log──► Debezium ──► Kafka brokers ◄── reads per partition ── Spark executors    │
     ▲                                                                              │  ▲          │
     │ JDBC (batch and DW delivery)                                                 │  │ tasks    │
     └──────────────────────────────────────────────────────────────────────────────┘  │          │
                                                                                Spark driver      │
                                                                                (Airflow, Jupyter)│
                                                                                       │          │
          Polaris ◄────────── "what is the table's current version?" / commit ─────────┤          │
             │                                                                         │          │
          Postgres                         Trino coordinator ──► Trino workers         │          │
                                                    │                 │                │          │
                                                    ▼                 ▼                ▼          │
                                         ┌─────────── SeaweedFS / S3 (files) ──────────────┐      │
                                         └─────────────────────────────────────────────────┘      │
                         └────────────────────────────────────────────────────────────────────────┘
```

Three kinds of traffic:

1. **Metadata** (light, frequent): engines ↔ catalog; driver ↔ master; coordinator ↔ workers.
2. **Data** (heavy): executors and workers ↔ object storage; consumers ↔ Kafka.
3. **Node-to-node exchange** (the most expensive): Spark shuffle and data exchange between Trino workers. This is where the network becomes the bottleneck once you distribute.

Rules that hold at any scale:

- Every processing node must reach the storage, the catalog and the driver or coordinator. Names and ports must work **on the network**, not only inside one machine.
- **Shared state moves off local disk:** checkpoints, files and logs go to object storage or a database. A process that keeps state on local disk does not scale horizontally.
- **Stateless components scale easily** (executors, Trino workers, catalog replicas). **Stateful components** (Kafka, Postgres, storage) need replication and care.
- **Synchronized clocks** (NTP): S3, Kafka and logs depend on correct time across machines.

### The typical growth path

| Phase | Scenario | Topology |
|---|---|---|
| 1 | POC, one server | Docker Compose. Tune cores and memory. **This POC.** |
| 2 | Not enough processing capacity | More machines as Spark or Trino workers. Checkpoints on S3. Firewall and authentication between nodes |
| 3 | High availability | Kafka with 3 brokers, replicated Postgres, distributed storage (clustered SeaweedFS, Ceph), several catalog replicas |
| 4 | Many teams and jobs | Kubernetes (k3s on-prem): Spark pods per job, autoscaling, Helm charts, GitOps |
| Cloud | On-demand elasticity | S3/ADLS/GCS, managed engines (EMR, Databricks, Athena), pay per use |

The open format (Parquet + Iceberg + REST catalog) is what makes it possible to move from one phase to the next **without migrating the data**: what changes is where the engines run, not how the data is stored.

---

## 13. From data mart to AI

Useful AI in a company depends far more on **reliable, well-described data** than on the model. This architecture is the foundation for it.

### What AI needs from the platform

| Need | How this architecture meets it |
|---|---|
| Correct, reconciled data | Silver identical to the source; ERP × lake × DW reconciliation |
| Full history | Bronze keeps everything; Iceberg *time travel* |
| **Point-in-time correctness** | To train a forecasting model, each example may only use what was known **at that moment**. Iceberg snapshots and SCD type 2 prevent leaking information from the future (*data leakage*) |
| Meaning of the data | Gold with business names, a defined grain, column documentation |
| Controlled access | Catalog and DW permissions; personal data out of gold |
| Continuous updates | Streaming and scheduled loads keep gold fresh |

### Usage patterns

- **Classic machine learning** (demand forecasting, churn, anomaly detection): trains on gold or on a **feature store** (a table of attributes per entity and date, computed in the lake).
- **Generative AI querying data (*text-to-SQL*):** the model writes SQL from a question in natural language. It only works well on top of a clear **semantic** model: gold tables with business names, descriptions, defined metrics ("net revenue = ..."). On bronze, results are poor.
- **RAG** (*Retrieval-Augmented Generation*): documents (manuals, contracts, tickets) are split into chunks, turned into vectors (*embeddings*) and searched by similarity to give the model context. The lake stores the documents and the vectors; the pipeline that keeps them up to date is data engineering.
- **Agents:** combine both: they query gold and the documents to answer and act. Access control must apply to them too.

Core message: **the gold layer, documented and tested, is the product.** BI, ML and generative AI are its consumers.

---

## 14. What is expected from a mid-level and a senior engineer

### Common foundation (every data engineer)

- **Advanced SQL:** window functions, CTEs, joins and their cost, reading execution plans.
- **Python** for pipelines: libraries, tests, packaging.
- **Modeling:** normalization (OLTP) and the dimensional model (OLAP), grain, SCD.
- **Git** and code review; **Linux**, basic networking (DNS, ports, firewall) and **containers**.
- **Formats:** CSV, JSON, Avro, Parquet; when to use each.

### Mid-level: builds and runs reliable pipelines

- Writes Spark and SQL jobs that are **idempotent**, incremental and reprocessable.
- Reads the Spark UI and identifies shuffle, skew and spill.
- Understands Kafka (partition, offset, group) and CDC.
- Orchestrates with Airflow: dependencies, retries, backfill.
- Implements quality tests and reconciliation.
- Maintains Iceberg tables.
- Documents and operates: knows what to do when something fails (as in `OPERATIONS.md`).

### Senior: designs the system and makes the trade-offs

- **Chooses the architecture for the problem**, not for the hype: batch or streaming, lake or database, one machine or a cluster. Can say "we don't need that".
- **Thinks in failures:** what happens if each piece dies halfway? Where can there be duplicates, losses or half-written data?
- **Designs for change:** schema evolution, contracts with source systems, open formats to avoid vendor lock-in.
- **Sizes and costs:** estimates volume, growth, memory, storage and cloud or hardware cost.
- **Security and privacy** from the design stage, not as a patch.
- **Translates to the business:** turns "we need data" into defined metrics, freshness targets and owners. Measures the value delivered.
- **Standardizes and teaches:** job templates, naming conventions, code review, documentation that lets someone else operate alone.

### Questions a senior asks before building

1. Which business decision will this data support, and how often is it made?
2. What is the source of truth, and who owns it?
3. What is the volume today and in 3 years?
4. What delay is acceptable: seconds, minutes, hours, a day?
5. What happens if the data is wrong or late? Who notices?
6. Who may see this data? Does it contain personal data?
7. Who will operate this at 3 a.m.?

---

## 15. Common anti-patterns

| Anti-pattern | Why it is bad | Instead |
|---|---|---|
| Heavy analytics directly on the ERP database | Brings the company's operations down | CDC or a replica + lake/DW |
| Business rules scattered (job, view, BI) | Diverging numbers, nobody trusts them | One place, versioned and tested |
| Blind `INSERT` in a job that may rerun | Duplicates on every failure | MERGE or partition overwrite |
| Thousands of small files | Slow queries, heavy catalog | Compaction and a proper file size |
| Partitioning by a high-cardinality column (e.g. `customer_id`) | Explosion of partitions and files | Partition by date; sort or *bucket* for the rest |
| Streaming "because it is modern" | Cost and complexity with no gain | Batch, until latency is a real requirement |
| A distributed cluster for 20 GB | Complexity and cost for no reason | One machine; DuckDB or local Spark |
| No tests and no reconciliation | Errors discovered by the director | Tests in the pipeline and checks against the source |
| `SELECT *` and `collect()` on a large table | Reads and pulls too much data | Project columns, aggregate in the engine |
| Secrets in code or in git | Leaks | `.env` out of git, a secrets vault |
| Tool before the problem | Expensive platform nobody uses | Start from the business question |

---

## 16. Study path

### Books

- *Fundamentals of Data Engineering* — Joe Reis and Matt Housley. The big-picture map of the profession; the data engineering lifecycle.
- *Designing Data-Intensive Applications* — Martin Kleppmann. Distributed systems for people who work with data: replication, partitioning, transactions, streams. Senior-level reading.
- *The Data Warehouse Toolkit* — Ralph Kimball and Margy Ross. The reference for dimensional modeling.
- *Spark: The Definitive Guide* — Bill Chambers and Matei Zaharia. Spark foundations (the APIs have changed since, the concepts have not).
- *Kafka: The Definitive Guide* — Gwen Shapira and others.

### Official documentation worth reading end to end

- The Apache Iceberg specification (iceberg.apache.org/spec): understanding the metadata tree changes how you think about tables.
- Spark *Tuning* and *Monitoring* (spark.apache.org/docs).
- Trino concepts (trino.io/docs): coordinator, workers, connectors.
- The Debezium documentation for SQL Server.

### Exercises with this POC

1. Open the Spark UI (port 4040) during a job and identify the stages and shuffles.
2. Create skew on purpose (many orders from one customer) and watch its effect on a join.
3. Query silver with *time travel* in Trino before and after a simulator run.
4. Compare `daily_sales` with and without compacting silver.
5. Read the same gold table from Spark, Trino and DuckDB and compare times and plans.
6. Implement SCD type 2 on `dim_customer` (state change) and see the impact on historical sales.
7. Write quality tests (uniqueness, nulls, integrity) for gold and make one of them fail.
8. Stop the CDC job in the middle of activity and check that silver is still identical once it comes back.
