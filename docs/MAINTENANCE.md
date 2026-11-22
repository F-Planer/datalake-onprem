# Table maintenance, compression and late data

How an Iceberg lakehouse stays fast and small over time, and how this
project deals with data that arrives late. It explains the concepts and
points to the code that implements them:

| What | Where |
|---|---|
| Maintenance job | `jobs/maintenance/iceberg_maintenance.py` |
| Maintenance schedule (3 a.m. daily) | `airflow/dags/lakehouse_maintenance.py` |
| Late-data pattern (watermark + recompute touched days) | `jobs/gold/orders_daily.py`, DAG `orders_events_hourly` |
| Test data: history, live activity, late events | `config/sqlserver/02-simulator.sql`, `history.sh`, `producers/orders/producer.py` |

Day-to-day commands are in [OPERATIONS.md](OPERATIONS.md#5-test-data-and-maintenance).

---

## 1. Why a lakehouse needs maintenance

Parquet files are **immutable**. Iceberg never edits a file: every write
(append, MERGE, overwrite) adds new files and a new **snapshot** that says
which files make up the table from now on.

```
catalog (Polaris)  ->  metadata.json  ->  snapshot  ->  manifest list  ->  manifests  ->  data files
                       (schema,           (one per      (one per          (lists of      (Parquet)
                        partitioning,      commit)       snapshot)          files, with    + delete files
                        snapshot list)                                      min/max stats)
```

That design gives atomic commits, time travel and safe concurrent readers,
but it has three side effects that grow with every commit:

1. **Small files.** The streaming and CDC jobs commit every 30–60 seconds.
   Each commit writes at least one file per partition touched. A table fed
   all day by streaming ends up with thousands of files of a few KB.
   Every query has to open each one (S3 request, Parquet footer, statistics),
   so a query that should read 1 file reads 3,000.
2. **Delete files.** The silver tables use *merge-on-read*: a MERGE does not
   rewrite the Parquet file that contains the changed row; it writes a small
   *position delete file* saying "row 1,234 of file X is gone". Writes are
   fast, but every read has to apply the deletes on the fly.
3. **Old snapshots and orphan files.** Every snapshot keeps its files alive
   on disk, even after newer snapshots replaced them. A write that fails
   halfway leaves files that no snapshot points to (*orphans*).

**Maintenance is the routine that fixes the three**, without changing a
single row of data. In this project it runs every night.

---

## 2. The five routines

They run in this order for every table (`iceberg_maintenance.py`):

### 2.1 Compaction: `rewrite_data_files`

Reads the small files and writes them back as few large ones (target:
128 MB here; Iceberg's default is 512 MB). It is a new snapshot like any
other: readers keep seeing the old files until the commit, then the new ones.

| Strategy | What it does | When |
|---|---|---|
| **bin-pack** (used here) | Only joins files, no sorting | Default. Cheap. Fixes small files |
| **sort** | Rewrites sorted by columns (`sort_order => 'created_at'`) | Columns used in most filters: min/max statistics become precise and queries skip more files |
| **z-order** | Sorts by several columns at once | Filters on 2–4 columns with no favourite |

Options used in the job:

- `min-input-files = 2`: a partition is only rewritten if it has at least 2 files;
- `delete-file-threshold = 1`: a data file with any delete file attached is
  rewritten, which **applies** the deletes (the deleted rows disappear for good);
- `partial-progress.enabled`: commits in groups, so a conflict loses only one group.

On a partitioned table you can limit it to old partitions, so it never touches
the one streaming is writing to right now:

```sql
CALL lakehouse.system.rewrite_data_files(
  table => 'lakehouse.bronze.orders',
  where => '_ingested_at < current_date');
```

### 2.2 Delete files: `rewrite_position_delete_files`

After compaction, the remaining delete files are merged, and the ones that
point to files that no longer exist (*dangling*) are dropped.

**Copy-on-write vs merge-on-read**, the trade-off behind delete files:

| | Copy-on-write | Merge-on-read (silver here) |
|---|---|---|
| A MERGE that changes 1 row | rewrites the whole data file | writes a tiny delete file |
| Write cost | high | low |
| Read cost | none | applies the deletes on every read |
| Needs compaction | less | **yes**, regularly |

CDC tables get MERGEs every minute, so merge-on-read + nightly compaction
is the usual choice. A table rewritten once a day (gold) does not need it.

### 2.3 Manifests: `rewrite_manifests`

Every commit adds a manifest. Thousands of small manifests slow down
*query planning* (before any data is read). This groups them.

### 2.4 Snapshot expiry: `expire_snapshots`

Removes snapshots older than `--retain-days` (3 here), always keeping the
last `--retain-last` (5), and **deletes the files only they used**. This is
where disk space actually comes back after a compaction: the old small
files belong to old snapshots.

Trade-offs:

- **Time travel.** You can only query the table "as of" a moment inside
  the retention window: `SELECT ... FOR TIMESTAMP AS OF ...` older than 3
  days no longer works.
- **Streaming readers.** The CDC job reads the bronze tables as a stream,
  and its checkpoint stores the last snapshot it processed. If the job stays
  stopped for longer than the retention and that snapshot is expired, the
  stream cannot resume. **Rule: retention > the longest a streaming job may
  stay stopped.** If it happens, the fix is rebuilding silver from bronze
  (delete the silver checkpoint and table; the job recreates both).

### 2.5 Orphan files: `remove_orphan_files`

Lists every file under the table's folder and deletes the ones no snapshot
knows. **Only files older than `--orphan-days` (3)**: a job that is writing
right now has files that are not committed yet, and they would look like
orphans. Never lower this below the duration of your longest job.

### 2.6 What can go wrong while streaming is running

- **Appends** (bronze) never conflict with compaction: the new files are
  simply not part of the rewrite.
- **MERGE** (silver) can conflict: if compaction commits a new version of a
  file that a MERGE has just written deletes for, one of the two commits
  fails validation. Either compaction skips that group (next night it tries
  again) or the CDC job stops, its container restarts it, and it resumes from
  the checkpoint. The MERGE is idempotent, so no data is lost or duplicated.
- In the POC test (section 6) maintenance ran with the ERP simulator writing
  and no conflict happened; the rule above is what to expect when one does.
- That is still why maintenance runs at 3 a.m., when the hourly DAGs are idle.

### 2.7 The storage layer has its own garbage collection

When Iceberg deletes a file, SeaweedFS only **marks** it as deleted inside
its volume files; the disk space comes back when SeaweedFS *vacuums* the
volume. It does that on its own when a volume has more than 30% garbage. In
the POC test, expiry deleted 4,133 files, the disk only shrank after the
vacuum (139 MB → 98 MB). To run it by hand:

```bash
printf 'lock\nvolume.vacuum -garbageThreshold 0.1\nunlock\n' | \
  docker exec -i datalake-seaweedfs-1 weed shell
```

Ceph, MinIO and cloud object stores have equivalent background processes.
Lesson: "the table got smaller" and "the disk got emptier" are measured in
different places.

---

## 3. Compression and file layout

### 3.1 Inside a Parquet file

```
file
 └─ row group (≈128 MB of rows)        <- unit of parallelism
     └─ column chunk (one per column)  <- columnar: reads only the columns the query uses
         └─ pages (≈1 MB)              <- unit of encoding and compression
 footer: schema + min/max/null count per column and row group  <- lets readers skip
```

Two layers shrink the data:

1. **Encoding** (free, automatic): *dictionary* (a column with 5 distinct
   values stores 5 strings + small integers), *run-length* (`PAID, PAID,
   PAID` becomes `3 × PAID`), *delta* for sorted numbers and dates.
2. **Compression codec**, applied to each page after encoding:

| Codec | Ratio | Speed | Notes |
|---|---|---|---|
| snappy | lower | very fast | Old Spark default |
| **zstd** (used here) | high | fast | Best balance today; Iceberg's default since 1.4. Level 1–22 via `write.parquet.compression-level` (default 3) |
| gzip | high | slow | Legacy |
| lz4 | low | fastest | Hot, short-lived data |
| uncompressed | — | — | Never in a lake |

### 3.2 What really drives the size

The codec matters less than **how the data is laid out**:

- **Sorting.** Rows sorted by a low-cardinality column (status, channel,
  date) create long runs that encode to almost nothing, and make min/max
  statistics useful for skipping. `ALTER TABLE ... WRITE ORDERED BY
  created_at` makes every new write sorted; `rewrite_data_files` with
  `strategy => 'sort'` sorts what exists.
- **Types.** `DECIMAL` and `INT` compress far better than numbers stored as
  `STRING`; a `DATE` is 4 bytes, the text `'2026-11-22'` is 10.
- **File size.** Few large files compress better (bigger dictionaries,
  fewer footers) and are read faster. That is compaction again.
- **Partitioning.** Partition only by columns used in filters and with
  few values (day, not second; state, not customer). A partition should
  hold hundreds of MB at least. Over-partitioning is the classic way to
  create small files by design. Iceberg's *hidden partitioning*
  (`days(created_at)`) means queries filter on `created_at` and Iceberg
  prunes partitions on its own.

### 3.3 Measuring it (Trino, in DBeaver)

```sql
-- files, average size and bytes per row of a table
SELECT count(*)                                AS files,
       round(avg(file_size_in_bytes) / 1024)   AS avg_kb,
       sum(record_count)                       AS rows,
       round(sum(file_size_in_bytes) * 1.0 / sum(record_count), 1) AS bytes_per_row
FROM lakehouse.silver."erp_orders$files"
WHERE content = 0;          -- 0 = data, 1 = position deletes

-- snapshots (one per commit)
SELECT count(*), min(committed_at), max(committed_at)
FROM lakehouse.silver."erp_orders$snapshots";
```

---

## 4. Late data

### 4.1 Two clocks

Every record has (at least) two times:

- **event time**: when it happened (`created_at` of the order);
- **ingestion time**: when it reached the lake (`_ingested_at`, `_kafka_timestamp`).

Data is **late** when the gap between them is larger than the pipeline
expects. It is normal, not an exception:

| Source of lateness | In this project |
|---|---|
| Offline devices (store terminal, phone app) send their buffer later | Producer `--late-ratio 0.05` (5% of the events 1–72 h late) |
| Back-dated entries in the ERP (an order typed in today with yesterday's date) | `SimulateActivity` syncs an offline-store order 1–72 h old in ~10% of the calls |
| Pipeline downtime: Kafka keeps the events and the job catches up later | Stop `spark-streaming-bronze` for 30 min and start it again |
| Long transactions that commit after a batch read | `--lookback-minutes` in the incremental batch load |

The question is never "how do I avoid late data" but **"which tables
change when it arrives, and how do I update them cheaply and correctly"**.

### 4.2 Layer by layer

**Bronze: partition by ingestion time.** `bronze.orders` is partitioned by
`days(_ingested_at)`. A late event is appended to *today's* partition,
like any other. Bronze never rewrites old files, and old partitions stay
compacted. If bronze were partitioned by event day, every late event would
create a tiny file in an old, already compacted partition.

**Silver (CDC): order by the source, not by arrival.** MERGE by primary key,
keeping the latest change per key by Kafka offset (Debezium sends all
changes of a key to the same partition, in commit order). A change that
arrives late is still applied in the right order. For incremental batch
loads, the `--lookback-minutes` window re-reads the last minutes, and the
`WHEN MATCHED AND s.UpdatedAt >= t.UpdatedAt` guard never lets an older
version overwrite a newer one.

**Gold: the aggregate by event day is what changes.** A late order from
Tuesday changes Tuesday's revenue. There are four strategies:

| Strategy | How | Pros | Cons | Here |
|---|---|---|---|---|
| **Full rebuild** | Recompute everything every run | Simple, always correct | Cost grows with history | `jobs/gold/sales.py` (fine for this volume) |
| **Fixed lookback** | Recompute the last N days every run | Simple, predictable cost | Data later than N days is never fixed | — |
| **Watermark + touched partitions** | Find which event days the new rows touch; recompute only those | Correct for any delay, cost ∝ change | Needs a control table | `jobs/gold/orders_daily.py` |
| **Streaming watermark** | Spark `withWatermark('created_at', '2 hours')`: events later than that are **dropped** | Real time, bounded state | Very late data is lost; usually paired with a batch correction | Explained only |

### 4.3 The pattern in `orders_daily.py`

```
run N:   watermark = 12:00
         new rows  = bronze WHERE _ingested_at > 12:00         (partition pruning: reads only today)
         days      = distinct to_date(created_at) of new rows   -> {Nov 22, Nov 20, Nov 19}
         recompute = bronze WHERE to_date(created_at) IN days   (file statistics skip the rest)
                     dedup by order_id, aggregate
         write     = overwritePartitions()                      (replaces only those 3 days)
         watermark = max(_ingested_at) seen                     (only after the gold commit)
```

Why each detail matters:

- **The watermark is saved after the gold commit.** If the job dies in
  between, the next run recomputes the same days: same result. That is
  idempotency, the property that makes retries safe.
- **The watermark is the max value seen, not the clock.** A micro-batch
  still being committed during the read has a later `_ingested_at` and is
  picked up next time. Using `now()` would skip it forever.
- **Recompute, do not add.** Adding the late rows' revenue to the stored
  total breaks on duplicates (Kafka is at-least-once) and on retries.
  Recomputing the whole day from bronze with dedup is always right.
- **`overwritePartitions()`** writes new files only for the touched days.
  In Parquet terms: late data costs a few new files in a few partitions,
  not a rewrite of the table.
- `late_orders` and `max_delay_hours` in the output make lateness visible.

### 4.4 Consequences for the business

- **Past days can change.** Tableau shows a revenue for Tuesday on
  Wednesday morning, and a slightly different one on Friday. This must be
  agreed upon and documented, or users will distrust the numbers.
- **Closing periods.** Finance usually "closes" a month: after day 5 of the
  next month, late data no longer changes it and goes into an adjustment.
  In code that is a limit on the touched days (`days >= close_date`).
- **Measure the lateness** before choosing a strategy:

```sql
-- distribution of the delay of the Kafka events (Trino)
SELECT CASE WHEN lag_min < 1 THEN 'on time (< 1 min)'
            WHEN lag_min < 60 THEN '1-60 min'
            WHEN lag_min < 1440 THEN '1-24 h'
            ELSE '> 24 h' END AS delay,
       count(*) AS events
FROM (SELECT date_diff('second', created_at, _kafka_timestamp) / 60.0 AS lag_min
      FROM lakehouse.bronze.orders)
GROUP BY 1 ORDER BY 2 DESC;
```

---

## 5. Production checklist

- [ ] Maintenance scheduled for every table, with alerts if it fails.
- [ ] Snapshot retention longer than the longest acceptable streaming downtime.
- [ ] Orphan-file age longer than the longest job.
- [ ] Files per partition and average file size monitored (query 3.3).
- [ ] Partitioning reviewed: no partition with only a few MB.
- [ ] Event time and ingestion time stored in every bronze table.
- [ ] For each gold table: which strategy for late data, written down.
- [ ] Business users told which numbers can change, and until when.

---

## 6. Results in this POC (2026-11-22)

**Volume:** 100,000 historical orders (180 days) + live simulator; 122,000
Kafka events, 5% of the first 100,000 up to 72 h late. About 1 million rows
in the lake.

**Small files after 30 minutes of streaming and CDC, and after maintenance**
(`lakehouse_maintenance` DAG, 2 minutes for 27 tables, with the ERP
simulator writing at the same time):

| Table | Before | After |
|---|---|---|
| `bronze.orders` (Kafka, 122k rows) | 61 files, avg 57 KB, 62 snapshots | 1 file of 1.7 MB (+1 written during the run) |
| `bronze.erp_orders` (CDC) | 67 files, avg 94 KB | 1 file of 3 MB (+1) |
| `bronze.erp_stock` | 66 files, avg 11 KB | 1 file of 81 KB (+1) |
| `silver.erp_orders` (merge-on-read) | 35 data + 33 delete files | 1 data file, 0 delete files |
| `silver.erp_stock` | 34 data + 33 delete files, 4,639 physical rows for 510 real ones | 1 file, 510 rows |
| `gold.fact_sales_items` | 78 snapshots (hourly rebuilds) | 1 snapshot after expiry with retention 0, 148 old files freed |

- No row changed: silver stayed identical to the ERP (count and sum) in the
  5 CDC tables, and `bronze.orders` kept its 122,136 rows.
- The CDC job kept running: no conflict and no restart during the run.
- Snapshot expiry with the default 3 days freed nothing (all snapshots were
  younger); with retention 0 on silver and gold it freed about 4,000 files.
  Disk space only came back after the SeaweedFS vacuum (section 2.7).
- The silver rebuild procedure (OPERATIONS.md, section 3) was tested on
  `erp_stock`: recreated from the compacted bronze, identical to the source.

**Late data** (`orders_daily.py`):

- 100,000 events, of which 4,308 for a previous day: 4 event days recomputed.
- Then 3,000 events, all late (up to 48 h): only Nov 20–22 were recomputed;
  Nov 19 was not touched (same `_computed_at`). The Iceberg commit replaced 3
  files in 3 partitions out of 4.
- Total orders in gold = 105,000 = 2,000 + 100,000 + 3,000: no duplicates.
