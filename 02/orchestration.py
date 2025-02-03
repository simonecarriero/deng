import dagster as dg
from datetime import datetime
import nyc_taxi
import os


def conn():
    username = os.environ["DB_USERNAME"]
    password = os.environ["DB_PASSWORD"]
    host = os.environ["DB_HOST"]
    port = os.environ["DB_PORT"]
    database = os.environ["DB_DATABASE"]
    return f"postgresql://{username}:{password}@{host}:{port}/{database}"


partitions = dg.MultiPartitionsDefinition(
    {
        "month": dg.MonthlyPartitionsDefinition(start_date="2019-01-01"),
        "taxi_type": dg.StaticPartitionsDefinition(["yellow", "green"]),
    }
)


@dg.asset(partitions_def=partitions)
def ingest_taxi_db(context):
    [date, taxi_type] = context.partition_key.split("|")
    date = datetime.strptime(date, "%Y-%m-%d")
    file = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{date.year}-{date.month:02d}.parquet"
    nyc_taxi.ingest_taxi_db(conn(), taxi_type, file)


@dg.asset
def ingest_zones_db():
    nyc_taxi.ingest_zones_db(conn(), "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv")


defs = dg.Definitions(
    assets=[
        ingest_taxi_db,
        ingest_zones_db,
    ]
)
