import datetime

import pytest
import sqlalchemy
from testcontainers.postgres import PostgresContainer

from nyc_taxi import ingest_taxi_db, ingest_zones_db


@pytest.fixture(scope="module")
def postgres_container():
    container = PostgresContainer("postgres:16")
    container.start()
    yield container
    container.stop()


def test_ingest_taxi_db(postgres_container):
    file = "./data/green_tripdata_2019-10.sample.parquet"
    engine = sqlalchemy.create_engine(postgres_container.get_connection_url())
    with engine.connect() as conn:
        ingest_taxi_db(postgres_container.get_connection_url(), "green", file)
        result = conn.execute(sqlalchemy.text("select * from ny_taxi_data_green limit 1"))
        row = result.mappings().fetchall()[0]
        assert row["lpep_pickup_datetime"] == datetime.datetime(2019, 10, 1, 0, 26, 2)
        assert row["lpep_dropoff_datetime"] == datetime.datetime(2019, 10, 1, 0, 39, 58)
        assert row["PULocationID"] == 112
        assert row["DOLocationID"] == 196
        assert row["trip_distance"] == 5.88
        assert row["total_amount"] == 19.3
        assert row["tip_amount"] == 0
        assert row["unique_row_id"] == "7760027126549389027"


def test_ingest_taxi_db_idempotency(postgres_container):
    file = "./data/green_tripdata_2019-10.sample.parquet"
    engine = sqlalchemy.create_engine(postgres_container.get_connection_url())
    with engine.connect() as conn:
        ingest_taxi_db(postgres_container.get_connection_url(), "green", file)
        count_1 = conn.execute(
            sqlalchemy.text("select count(*) from ny_taxi_data_green")
        ).fetchall()[0][0]

        ingest_taxi_db(postgres_container.get_connection_url(), "green", file)
        count_2 = conn.execute(
            sqlalchemy.text("select count(*) from ny_taxi_data_green")
        ).fetchall()[0][0]

        assert count_1 == count_2


def test_ingest_zones_db(postgres_container):
    file = "./data/taxi_zone_lookup.csv.sample.csv"
    engine = sqlalchemy.create_engine(postgres_container.get_connection_url())
    with engine.connect() as conn:
        ingest_zones_db(postgres_container.get_connection_url(), file)
        result = conn.execute(sqlalchemy.text("select * from taxi_zone_lookup limit 1"))
        row = result.mappings().fetchall()[0]
        assert row["LocationID"] == 1
        assert row["Borough"] == "EWR"
        assert row["Zone"] == "Newark Airport"
        assert row["service_zone"] == "EWR"
