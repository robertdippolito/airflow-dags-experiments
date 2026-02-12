FROM postgres:16-alpine

ENV POSTGRES_DB=airflow \
    POSTGRES_USER=airflow \
    POSTGRES_PASSWORD=airflow

VOLUME ["/var/lib/postgresql/data"]

EXPOSE 5432

HEALTHCHECK --interval=10s --timeout=3s --retries=5 CMD pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"
