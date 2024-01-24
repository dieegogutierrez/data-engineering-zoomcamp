### Docker commands:

#### Run interactively ubuntu's image using bash tool
```bash
docker run -it ubuntu bash
```
#### Run interactively python's image using version 3.9
```bash
docker run -it python:3.9
```
#### Run interactively using python's image with version 3.9, overriding the entry point with bash
```bash
docker run -it --entrypoint=bash python:3.9
```
#### To use Dockerfile, you can build the image with a 'test' name and a 'pandas' version
```bash
docker build -t test:pandas .
```
#### Run postgres on docker
```bash
docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v /$(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    postgres:13
```
#### Accessing the database on terminal. CLI for postgres, the port is the one on host
```bash
pip install pgcli
pgcli -h localhost -p 5432 -u root -d ny_taxi
```
#### Unzip gz file
```bash
gunzip yellow_tripdata_2021-01.csv.gz
```
#### Count number of rows in csv file
```bash
wc -l yellow_tripdata_2021-01.csv
```
#### Get 100 rows of big csv file and save in new smaller file
```bash
head -n 100 yellow_tripdata_2021-01.csv > yellow_head.csv
```
#### Run pgAdmin on docker
```bash
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  dpage/pgadmin4
```
#### Running Postgres and pgAdmin together locally, otherwise use docker compose
#### Create a network
```bash
docker network create pg-network
```
#### Run Postgres 
```bash
docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v /$(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    --network=pg-network \
    --name pg-database \
    postgres:13
```
#### Run pgAdmin
```bash
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  --network=pg-network \
  --name pgadmin \
  dpage/pgadmin4
```
#### pgAdmin register server
General - name -any name
Connection - info from postgres database - name, username, password

#### Create connection with postgres using python. "postgresql://username:password@host:port/database_name". 
```bash
engine = create_engine("postgresql://root:root@localhost:5432/ny_taxi")
```
#### Convert jupyter notebook to script
```bash
jupyter nbconvert --to=script upload-data.ipynb
```
#### Ingesting data into postgres
```bash
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

python ingest_data.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips \
  --url=${URL}
```
#### Run all containers together in the network
```bash
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

docker run -it \
  --network=pg-network \
  taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url=${URL}
```
#### Run docker compose. To be able to still use the terminal run in dettach mode -d
```bash
docker-compose up
```
#### Stop docker compose
```bash
docker-compose down
```