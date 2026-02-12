from datetime import datetime

from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

PG_CONN_ID = "postgres_default"
RAW_SCHEMA = "social_raw"
RAW_TABLE = "students_social_media"
DATA_PATH = "/opt/airflow/datasets/students_social_media_addiction.csv"

@dag(
    dag_id="monolithic_transform_students",
    description="Monolithic SQL transform for students social media addiction data",
    default_args={"owner": "airflow"},
    tags=["demo", "sql", "students"],
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

def social_monolithic_sql():
    @task
    def load_raw_social_data() -> None:
        pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
        pg.run(f"create schema if not exists {RAW_SCHEMA}")
        pg.run(
            f"""
            drop table if exists {RAW_SCHEMA}.{RAW_TABLE};
            drop type if exists {RAW_SCHEMA}.{RAW_TABLE} cascade;
            """
        )
        pg.run(
            f"""
            create table {RAW_SCHEMA}.{RAW_TABLE} (
                Student_ID text,
                Age text,
                Gender text,
                Academic_Level text,
                Country text,
                Avg_Daily_Usage_Hours text,
                Most_Used_Platform text,
                Affects_Academic_Performance text,
                Sleep_Hours_Per_Night text,
                Mental_Health_Score text,
                Relationship_Status text,
                Conflicts_Over_Social_Media text,
                Addicted_Score text
            )
            """
        )

        pg.copy_expert(
            f"""
            copy {RAW_SCHEMA}.{RAW_TABLE} (
            Student_ID,
            Age,
            Gender,
            Academic_Level,
            Country,
            Avg_Daily_Usage_Hours,
            Most_Used_Platform,
            Affects_Academic_Performance,
            Sleep_Hours_Per_Night,
            Mental_Health_Score,
            Relationship_Status,
            Conflicts_Over_Social_Media,
            Addicted_Score
            ) from stdin with (format csv, header true)
            """,
            DATA_PATH,
        )
    
    monolithic_transform = SQLExecuteQueryOperator(
        task_id="monolithic_transform_students",
        conn_id=PG_CONN_ID,
        sql="sql/social_monolithic_transform_students.sql",
    )

    load_raw_social_data() >> monolithic_transform

dag = social_monolithic_sql()