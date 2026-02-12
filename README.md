# airflow-dags-experiments

Hands-on experiments comparing monolithic SQL in Airflow vs. dbt models + tests.
Two demo domains are included:

- Infrastructure cost data (seeded CSV, dbt models + tests)
- Student social media wellness data (monolith + dbt versions)

## Stack

- Airflow (Docker)
- Postgres (Docker)
- dbt (runs inside Airflow containers)
- Optional: DBeaver or Querybook for querying

## DAGs

- `infra_monolithic_sql`: monolithic SQL transform for infra costs.
- `infra_dbt_models`: dbt seed + run + test for infra costs.
- `monolithic_transform_students`: monolithic SQL transform for student social data.
- `social_dbt_models`: dbt seed + run + test for student social data.

## Data locations

- Student CSV: `datasets/students_social_media_addiction.csv`
- Infra dbt project: `dbt/infra_demo`
- Social dbt project: `dbt/social_demo`

## Run Airflow

Start:
```sh
docker compose up -d
```

Stop:
```sh
docker compose down --volumes --rmi all
```

## Trigger DAGs

Use the Airflow UI to trigger:
- `monolithic_transform_students`
- `social_dbt_models`
- `infra_monolithic_sql`
- `infra_dbt_models`

## Verify results (Postgres)

Monolith output:
```sql
select count(*) from social_mart.fct_platform_usage;
```

dbt output:
```sql
select count(*) from social_dbt.fct_platform_usage;
```

## Bad data demo (dbt value)

1) Add a few bad rows to `datasets/students_social_media_addiction.csv`.
2) Re-run `monolithic_transform_students` and `social_dbt_models`.
3) Observe: monolith still builds marts, dbt fails tests with clear error rows.

## Local dbt (optional)

If you run dbt locally, update `dbt/profiles.yml` to point to `localhost:5432`.
For Airflow runs, the host should remain `postgres`.
