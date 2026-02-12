Build the image
docker build -f database/Dockerfile -t local-postgres .

Create the volume
docker volume create postgres_data

Run the Database
docker run -d --name airflow-postgres \
  -p 5432:5432 \
  -v postgres_data:/var/lib/postgresql/data \
  -e POSTGRES_DB=airflow \
  -e POSTGRES_USER=airflow \
  -e POSTGRES_PASSWORD=airflow \
  local-postgres

Exec into the database
docker exec -it airflow-postgres psql -U airflow -d airflow


