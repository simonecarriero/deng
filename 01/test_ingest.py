import datetime

import pytest
import sqlalchemy
from testcontainers.postgres import PostgresContainer

from ingest import ingest


@pytest.fixture(scope="module")
def postgres_container():
    container = PostgresContainer("postgres:16")
    container.start()
    yield container
    container.stop()


def test_ingest_parquet(postgres_container):
    engine = sqlalchemy.create_engine(postgres_container.get_connection_url())
    with engine.connect() as conn:
        ingest(
            postgres_container.get_connection_url(),
            "green_tripdata",
            "./data/green_tripdata_2019-10.sample.parquet",
        )
        result = conn.execute(sqlalchemy.text("select * from green_tripdata limit 1"))
        row = result.mappings().fetchall()[0]
        assert row["lpep_pickup_datetime"] == datetime.datetime(2019, 10, 1, 0, 26, 2)
        assert row["lpep_dropoff_datetime"] == datetime.datetime(2019, 10, 1, 0, 39, 58)
        assert row["PULocationID"] == 112
        assert row["DOLocationID"] == 196
        assert row["trip_distance"] == 5.88
        assert row["total_amount"] == 19.3
        assert row["tip_amount"] == 0


def test_ingest_csv(postgres_container):
    engine = sqlalchemy.create_engine(postgres_container.get_connection_url())
    with engine.connect() as conn:
        ingest(
            postgres_container.get_connection_url(),
            "taxi_zone_lookup",
            "./data/taxi_zone_lookup.csv.sample.csv",
        )
        result = conn.execute(sqlalchemy.text("select * from taxi_zone_lookup limit 1"))
        row = result.mappings().fetchall()[0]
        assert row["LocationID"] == 1
        assert row["Borough"] == "EWR"
        assert row["Zone"] == "Newark Airport"
        assert row["service_zone"] == "EWR"
