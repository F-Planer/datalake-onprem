"""Iceberg table maintenance: compaction, delete files, snapshots and orphan files.

Why it is needed (details in docs/MAINTENANCE.md):
  - streaming and CDC commit every 30-60 s, and each commit writes new, small
    Parquet files. Thousands of small files make every query slower;
  - the merge-on-read silver tables do not rewrite data on MERGE: they write
    "delete files" that every read has to apply;
  - each commit creates a snapshot, and old snapshots keep old files alive
    on disk (they are what makes time travel possible);
  - a write that fails halfway leaves files no snapshot points to (orphans).

For each table of the given namespaces, in this order:
  1. rewrite_data_files            joins small files into large ones (bin-pack) and
                                   applies the delete files to the data
  2. rewrite_position_delete_files joins/drops the delete files that are left
  3. rewrite_manifests             reorganizes the metadata (manifest) files
  4. expire_snapshots              removes snapshots older than --retain-days (keeping at
                                   least --retain-last) and the files only they used
  5. remove_orphan_files           deletes files no snapshot knows, older than --orphan-days
It prints, per table, the number of files and snapshots before and after.

Running alongside streaming: appends (bronze) do not conflict with
compaction. A MERGE (silver) that commits on a file that compaction has just
rewritten can fail; one of the two retries: the compaction on the next run,
or the CDC job, which restarts from its checkpoint (idempotent MERGE).

Examples:
  submit-job.sh iceberg_maintenance.py --namespace silver
  submit-job.sh iceberg_maintenance.py --namespace bronze --table orders --dry-run
"""

import argparse
import time
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

CATALOG = "lakehouse"


def log(msg: str) -> None:
    print(f"[maintenance] {msg}", flush=True)


def stats(spark: SparkSession, table: str) -> dict:
    """Current state of the table, from the Iceberg metadata tables."""
    files = spark.sql(f"""
        SELECT
          count_if(content = 0)                         AS data_files,
          count_if(content <> 0)                        AS delete_files,
          coalesce(sum(if(content = 0, record_count, 0)), 0) AS records,
          coalesce(round(avg(if(content = 0, file_size_in_bytes, NULL)) / 1024, 1), 0) AS avg_kb,
          coalesce(round(sum(file_size_in_bytes) / 1048576, 2), 0) AS mb
        FROM {table}.files""").first().asDict()
    files["snapshots"] = spark.sql(f"SELECT count(*) FROM {table}.snapshots").first()[0]
    return files


def fmt(s: dict) -> str:
    return (f"{s['data_files']} data files (avg {s['avg_kb']} KB), {s['delete_files']} delete files, "
            f"{s['snapshots']} snapshots, {s['mb']} MB, {s['records']} rows")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--namespace", required=True, help="bronze, silver, gold, ops...")
    ap.add_argument("--table", help="only this table (default: all in the namespace)")
    ap.add_argument("--retain-days", type=int, default=3,
                    help="snapshots kept for time travel (default 3). Must be longer than "
                         "any streaming job may stay stopped: a stream that resumes from an "
                         "expired snapshot fails")
    ap.add_argument("--retain-last", type=int, default=5, help="minimum snapshots kept (default 5)")
    ap.add_argument("--orphan-days", type=int, default=3,
                    help="only files older than this are considered orphans (default 3); must "
                         "be longer than the longest write, or files of a running job are deleted")
    ap.add_argument("--target-file-mb", type=int, default=128, help="compacted file size (default 128)")
    ap.add_argument("--dry-run", action="store_true", help="only show the state and the orphan files")
    args = ap.parse_args()

    spark = SparkSession.builder.appName(f"maintenance_{args.namespace}").getOrCreate()
    now = datetime.now(timezone.utc)
    expire_before = (now - timedelta(days=args.retain_days)).strftime("%Y-%m-%d %H:%M:%S")
    orphan_before = (now - timedelta(days=args.orphan_days)).strftime("%Y-%m-%d %H:%M:%S")

    names = ([args.table] if args.table else
             [r.tableName for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{args.namespace}").collect()])
    log(f"{args.namespace}: {len(names)} table(s); snapshots kept {args.retain_days} days "
        f"(at least {args.retain_last}); orphans older than {args.orphan_days} days")

    failures = []
    for name in names:
        table = f"{CATALOG}.{args.namespace}.{name}"
        ident = table
        call = f"CALL {CATALOG}.system"
        start = time.time()
        before = stats(spark, table)
        log(f"{table} before: {fmt(before)}")
        if args.dry_run:
            orphans = spark.sql(f"""{call}.remove_orphan_files(table => '{ident}',
                older_than => TIMESTAMP '{orphan_before}', dry_run => true,
                prefix_listing => true)""").count()
            log(f"{table}: {orphans} orphan file(s) would be deleted")
            continue
        try:
            r = spark.sql(f"""{call}.rewrite_data_files(table => '{ident}', options => map(
                    'min-input-files', '2',
                    'delete-file-threshold', '1',
                    'target-file-size-bytes', '{args.target_file_mb * 1024 * 1024}',
                    'partial-progress.enabled', 'true',
                    'remove-dangling-deletes', 'true'))""").first()
            log(f"  rewrite_data_files: {r['rewritten_data_files_count']} -> {r['added_data_files_count']} files")

            if before["delete_files"]:
                r = spark.sql(f"{call}.rewrite_position_delete_files(table => '{ident}', "
                              f"options => map('rewrite-all', 'true'))").first()
                log(f"  rewrite_position_delete_files: {r['rewritten_delete_files_count']} -> "
                    f"{r['added_delete_files_count']} delete files")

            r = spark.sql(f"{call}.rewrite_manifests(table => '{ident}')").first()
            log(f"  rewrite_manifests: {r['rewritten_manifests_count']} -> {r['added_manifests_count']} manifests")

            r = spark.sql(f"""{call}.expire_snapshots(table => '{ident}',
                    older_than => TIMESTAMP '{expire_before}', retain_last => {args.retain_last})""").first()
            log(f"  expire_snapshots: {r['deleted_data_files_count']} data and "
                f"{r['deleted_position_delete_files_count']} delete files freed, "
                f"{r['deleted_manifest_files_count']} manifests")

            n = spark.sql(f"""{call}.remove_orphan_files(table => '{ident}',
                    older_than => TIMESTAMP '{orphan_before}', prefix_listing => true)""").count()
            log(f"  remove_orphan_files: {n} file(s) deleted")

            log(f"{table} after:  {fmt(stats(spark, table))}  ({time.time() - start:.0f}s)")
        except Exception as error:   # one table failing does not stop the others
            failures.append(name)
            log(f"{table} FAILED: {str(error).splitlines()[0][:300]}")

    spark.stop()
    if failures:
        raise SystemExit(f"maintenance failed for: {', '.join(failures)} (see the log above)")


if __name__ == "__main__":
    main()
