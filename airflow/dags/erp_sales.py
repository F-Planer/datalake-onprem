"""Sales ERP DAGs.

erp_sales_hourly (every hour):
    incremental load of the HOURLY_LOADS tables (SQL Server -> bronze -> silver)
    -> sales gold layer (which also uses the CDC silver: orders and items)
    -> delivery of the gold layer to the SQL Server DW

erp_sales_daily (1 a.m., Sao Paulo time):
    full load of every table (HOURLY_LOADS and DAILY_LOADS).
    For the hourly tables this is the reconciliation: it sees rows deleted at
    the source, which the incremental load misses.

TO ADD A TABLE: add a line to HOURLY_LOADS or DAILY_LOADS
(source name -> primary key). See docs/OPERATIONS.md.

Each task runs a Spark job on the cluster through /opt/spark/submit-job.sh,
which injects the credentials from the container environment.
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

JOBS = "/opt/airflow/jobs"
SUBMIT = "/opt/spark/submit-job.sh"

# Scheduled-load tables: source name (dbo schema) -> primary key.
# Composite key: "Column1,Column2".
#
# HOURLY_LOADS: incremental every hour + full every day. The table MUST have
# the UpdatedAt column (date/time of the last change).
HOURLY_LOADS = {
    "Customers": "CustomerId",
    "Products": "ProductId",
}
# DAILY_LOADS: full only, once a day. For small tables or tables without a
# change-date column.
DAILY_LOADS = {
    "Categories": "CategoryId",
    "Carriers": "CarrierId",
}
TABLES = {**HOURLY_LOADS, **DAILY_LOADS}

DEFAULTS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}


def load(table: str, mode: str) -> BashOperator:
    return BashOperator(
        task_id=f"load_{table.lower()}_{mode}",
        bash_command=(f"{SUBMIT} {JOBS}/batch/erp_load.py "
                      f"--table {table} --key {TABLES[table]} --mode {mode}"),
        pool="spark",
    )


with DAG(
    dag_id="erp_sales_hourly",
    description="Incremental loads, sales gold layer and delivery to the SQL Server DW",
    schedule="0 * * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULTS,
    tags=["erp", "batch", "gold", "dw"],
) as hourly:
    gold = BashOperator(task_id="gold_sales", bash_command=f"{SUBMIT} {JOBS}/gold/sales.py", pool="spark")
    delivery = BashOperator(task_id="deliver_dw", bash_command=f"{SUBMIT} {JOBS}/delivery/gold_to_dw.py", pool="spark")
    [load(t, "incremental") for t in HOURLY_LOADS] >> gold >> delivery


with DAG(
    dag_id="erp_sales_daily",
    description="Full loads (small tables and reconciliation)",
    schedule="0 1 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULTS,
    tags=["erp", "batch"],
) as daily:
    for t in TABLES:
        load(t, "full")
