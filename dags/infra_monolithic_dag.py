# SPDX-License-Identifier: Apache-2.0
"""Monolithic SQL demo DAG for infrastructure cost data."""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DATA_PATH = "/opt/airflow/dbt/infra_demo/seeds/infra_costs.csv"
PG_CONN_ID = "postgres_default"
SCHEMA = "infra_demo"
RAW_TABLE = "raw_infra_costs"


@dag(
    dag_id="infra_monolithic_sql",
    description="Monolithic SQL transform for infra costs (Airflow only).",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"owner": "airflow"},
    tags=["demo", "sql", "infra"],
)
def infra_monolithic_sql():
    @task
    def load_raw_costs() -> None:
        pg = PostgresHook(postgres_conn_id=PG_CONN_ID)

        pg.run(f"create schema if not exists {SCHEMA}")
        pg.run(f"drop table if exists {SCHEMA}.{RAW_TABLE}")
        pg.run(
            f"""
            create table {SCHEMA}.{RAW_TABLE} (
              usage_date text,
              account_id text,
              service text,
              region text,
              resource_id text,
              usage_hours text,
              cost_usd text,
              team text,
              env text
            )
            """
        )

        pg.copy_expert(
            f"""
            copy {SCHEMA}.{RAW_TABLE} (
              usage_date,
              account_id,
              service,
              region,
              resource_id,
              usage_hours,
              cost_usd,
              team,
              env
            ) from stdin with (format csv, header true)
            """,
            DATA_PATH,
        )

    monolithic_transform = SQLExecuteQueryOperator(
        task_id="monolithic_transform",
        conn_id=PG_CONN_ID,
        sql="sql/infra_monolithic_transform.sql",
    )

    load_raw_costs() >> monolithic_transform


dag = infra_monolithic_sql()
