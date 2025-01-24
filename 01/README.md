# Dev

## Install dependencies
```
uv sync
```

## Run tests
```
uv run pytest
```

## Lint and format
```
uv run check --fix
uv run format
```

# Local ingest

## Start postgres
```
docker run \
  -e POSTGRES_USER=root \
  -e POSTGRES_PASSWORD=password \
  -e POSTGRES_DB=ny_taxi \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 postgres:17
```

## Build the ingest image
```
docker build -t ingest .
```

## Ingest the green taxi trips from October 2019
```
docker run -t \
  -e DB_USERNAME=root \
  -e DB_PASSWORD=password \
  -e DB_HOST=localhost \
  -e DB_PORT=5432 \
  -e DB_DATABASE=ny_taxi \
  -e TABLE_NAME=ny_taxi_data \
  -e FILE=https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-10.parquet \
  --network host \
  ingest
```

## Ingest the taxi zone lookup
```
docker run -t \
  -e DB_USERNAME=root \
  -e DB_PASSWORD=password \
  -e DB_HOST=localhost \
  -e DB_PORT=5432 \
  -e DB_DATABASE=ny_taxi \
  -e TABLE_NAME=zones \
  -e FILE=https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv \
  --network host \
  ingest
```

# Homeworks

## Question 1. Understanding docker first run
Run docker with the `python:3.12.8` image in an interactive mode, use the entrypoint `bash`.
What's the version of `pip` in the image?

```
24.3.1
```

## Question 2. Understanding Docker networking and docker-compose
Given the following `docker-compose.yaml`, what is the `hostname` and `port` that **pgadmin** should use to connect to the postgres database?

```yaml
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin  

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

```
db:5432
```

## Question 3. Trip Segmentation Count
During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, **respectively**, happened:
1. Up to 1 mile
2. In between 1 (exclusive) and 3 miles (inclusive),
3. In between 3 (exclusive) and 7 miles (inclusive),
4. In between 7 (exclusive) and 10 miles (inclusive),
5. Over 10 miles 

```
select count(case when trip_distance <= 1 then 1 end),
       count(case when trip_distance > 1 and trip_distance <= 3 then 1 end),
       count(case when trip_distance > 3 and trip_distance <= 7 then 1 end),
       count(case when trip_distance > 7 and trip_distance <= 10 then 1 end),
       count(case when trip_distance > 10 then 1 end)
from ny_taxi_data 
where lpep_pickup_datetime::date >= '2019-10-1'
and lpep_dropoff_datetime::date < '2019-11-1'
```

```
104802 198924 109603 27678 35189
```

## Question 4. Longest trip for each day
Which was the pick up day with the longest trip distance?
Use the pick up time for your calculations.

Tip: For every day, we only care about one single trip with the longest distance. 

- 2019-10-11
- 2019-10-24
- 2019-10-26
- 2019-10-31

```
select lpep_pickup_datetime::date
from ny_taxi_data
order by trip_distance desc
limit 1
```

```
2019-10-31
```

## Question 5. Three biggest pickup zones

Which were the top pickup locations with over 13,000 in
`total_amount` (across all trips) for 2019-10-18?

Consider only `lpep_pickup_datetime` when filtering by date.
 
- East Harlem North, East Harlem South, Morningside Heights
- East Harlem North, Morningside Heights
- Morningside Heights, Astoria Park, East Harlem South
- Bedford, East Harlem North, Astoria Park

```
select z."Zone", sum(total_amount) 
from ny_taxi_data ntd 
join zones z on z."LocationID" = ntd."PULocationID" 
where lpep_pickup_datetime::date = '2019-10-18'
group by z."LocationID", z."Zone" 
having sum(total_amount) > 13000
order by sum(total_amount) desc
```

```
East Harlem North 18686.679999999724
East Harlem South 16797.259999999824
Morningside Heights 13029.789999999923
```

## Question 6. Largest tip

For the passengers picked up in October 2019 in the zone
name "East Harlem North" which was the drop off zone that had
the largest tip?

Note: it's `tip` , not `trip`

We need the name of the zone, not the ID.

- Yorkville West
- JFK Airport
- East Harlem North
- East Harlem South

```
select do_z."Zone" 
from ny_taxi_data ntd
join zones pu_z on pu_z."LocationID" = ntd."PULocationID"
join zones do_z on do_z."LocationID" = ntd."DOLocationID" 
where pu_z."Zone" = 'East Harlem North'
order by ntd.tip_amount desc 
limit 1
```

```
JFK Airport
```

## Question 7. Terraform Workflow

Sequence that, **respectively**: 
1. Downloading the provider plugins and setting up backend,
2. Generating proposed changes and auto-executing the plan
3. Remove all resources managed by terraform`

Answers:
- terraform import, terraform apply -y, terraform destroy
- teraform init, terraform plan -auto-apply, terraform rm
- terraform init, terraform run -auto-approve, terraform destroy
- terraform init, terraform apply -auto-approve, terraform destroy
- terraform import, terraform apply -y, terraform rm

```
terraform init, terraform apply -auto-approve, terraform destroy
```
