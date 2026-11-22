"""Daily Iceberg maintenance of every lake table (3 a.m., Sao Paulo time).

Per namespace, one after the other: compaction of small files, cleanup of
delete files, manifests, expiry of old snapshots and removal of orphan files.
See jobs/maintenance/iceberg_maintenance.py and docs/MAINTENANCE.md.

It runs at night, when the hourly DAGs are idle. The streaming jobs keep
running; a conflict with a CDC MERGE is retried (see the job docstring).
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

NAMESPACES = ["bronze", "silver", "gold", "ops"]
RETAIN_DAYS = 3   # time travel window; also the longest a streaming job may stay stopped

with DAG(
    dag_id="lakehouse_maintenance",
    description="Iceberg compaction, snapshot expiry and orphan-file cleanup",
    schedule="0 3 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(hours=2),
    },
    tags=["maintenance", "iceberg"],
) as dag:
    previous = None
    for ns in NAMESPACES:
        task = BashOperator(
            task_id=f"maintain_{ns}",
            bash_command=("/opt/spark/submit-job.sh /opt/airflow/jobs/maintenance/iceberg_maintenance.py "
                          f"--namespace {ns} --retain-days {RETAIN_DAYS}"),
            pool="spark",
        )
        if previous:
            previous >> task
        previous = task
