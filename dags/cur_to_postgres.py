import logging
from datetime import datetime
import os
import tempfile

import pyarrow.parquet as pq
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

BUCKET = "robs-cur-report-bucket"
# Prefix to the CUR data (no leading slash). Adjust BILLING_PERIOD as needed.
BASE_PREFIX = "cost-reports/robs-cur-data/data/"
BILLING_PERIOD = "2025-12"  # optional partition selector; set to "" to scan all
TABLE = "cur_raw"
TMP_DIR = "/tmp"
AWS_CONN_ID = "aws_default"
PG_CONN_ID = "postgres_default"


def _build_prefix() -> str:
    base = BASE_PREFIX.lstrip("/").rstrip("/")
    if BILLING_PERIOD:
        return f"{base}/BILLING_PERIOD={BILLING_PERIOD.strip('/')}/"
    return f"{base}/"


with DAG(
    dag_id="cur_s3_to_postgres",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
) as dag:

    @task
    def download_cur() -> str:
        logger = logging.getLogger(__name__)
        s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
        prefix = _build_prefix()
        logger.info(
            "Listing S3 with bucket=%s base_prefix=%s billing_period=%s resolved_prefix=%s",
            BUCKET,
            BASE_PREFIX,
            BILLING_PERIOD,
            prefix,
        )
        keys = [
            key
            for key in s3.list_keys(bucket_name=BUCKET, prefix=prefix) or []
            if key.endswith(".parquet")
        ]
        logger.info("Listing keys in bucket=%s under prefix=%s: %s", BUCKET, prefix, keys)
        if not keys:
            if BILLING_PERIOD:
                base_prefix_only = _build_prefix().rsplit("BILLING_PERIOD=", 1)[0]
                available_prefixes = s3.list_prefixes(
                    bucket_name=BUCKET,
                    prefix=base_prefix_only,
                    delimiter="/",
                ) or []
                logger.warning(
                    "No parquet files under prefix=%s. Available prefixes under base (%s): %s",
                    prefix,
                    base_prefix_only,
                    available_prefixes,
                )
            else:
                logger.warning("No parquet files under prefix=%s and no billing period filtering", prefix)
            raise ValueError(f"No CUR parquet files found under prefix {prefix}")

        latest_key = sorted(keys)[-1]
        local_path = os.path.join(TMP_DIR, os.path.basename(latest_key))
        s3.download_file(bucket_name=BUCKET, key=latest_key, local_path=local_path)
        return local_path

    @task
    def load_to_postgres(local_path: str) -> None:
        df = pq.read_table(local_path).to_pandas()

        with tempfile.NamedTemporaryFile(mode="w+", delete=False) as tmp_csv:
            df.to_csv(tmp_csv.name, index=False)
            tmp_csv.flush()

            pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
            with open(tmp_csv.name, "r") as csv_handle:
                pg.copy_expert(
                    f"COPY {TABLE} FROM STDIN WITH (FORMAT csv, HEADER true)",
                    csv_handle,
                )

    load_to_postgres(download_cur())
