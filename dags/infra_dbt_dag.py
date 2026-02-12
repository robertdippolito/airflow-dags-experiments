# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt/infra_demo"
DBT_PROFILES_DIR = "/opt/airflow/dbt"


@dag(
    dag_id="infra_dbt_models",
    description="dbt seed + models + tests for infra costs.",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"owner": "airflow"},
    tags=["demo", "dbt", "infra"],
)
def infra_dbt_models():
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=(
            "dbt seed --full-refresh "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "dbt run "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "dbt test "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_seed >> dbt_run >> dbt_test


dag = infra_dbt_models()
