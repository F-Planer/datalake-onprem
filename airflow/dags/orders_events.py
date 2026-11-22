"""Kafka `orders` events -> gold.orders_daily, every hour.

Incremental job that handles late events: it recomputes only the event days
touched by the rows ingested since the last run (watermark in
lakehouse.ops.watermarks). See jobs/gold/orders_daily.py and docs/MAINTENANCE.md.

To rebuild everything (after changing the business rule, for example):
trigger the DAG with the config {"full": true}.
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="orders_events_hourly",
    description="Daily aggregate of the Kafka order events, with late-data handling",
    schedule="15 * * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    params={"full": False},
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["streaming", "gold", "late-data"],
) as dag:
    BashOperator(
        task_id="gold_orders_daily",
        bash_command=("/opt/spark/submit-job.sh /opt/airflow/jobs/gold/orders_daily.py"
                      "{{ ' --full' if params.full else '' }}"),
        pool="spark",
    )
