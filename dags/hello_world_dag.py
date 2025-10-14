# SPDX-License-Identifier: Apache-2.0
"""
Example DAG to verify the Airflow environment.

This DAG runs a single task that logs a message. It is useful for confirming
that the scheduler can discover a DAG and execute tasks end-to-end.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="hello_world",
    description="Simple DAG that prints to console",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"owner": "airflow"},
    tags=["example"],
)
def hello_world():
    @task
    def say_hello() -> None:
        print("Hello from your first Airflow DAG!")

    say_hello()


dag = hello_world()
