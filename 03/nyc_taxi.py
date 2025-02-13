import random
import string
import gcsfs
import polars as pl
from google.cloud import bigquery


def key_columns():
    return [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "fare_amount",
        "trip_distance",
    ]


def write_gcs(df, object_uri):
    fs = gcsfs.GCSFileSystem()
    with fs.open(object_uri, mode="wb") as f:
        df.write_parquet(f)


def create_external_table_from_parquet(client, table_name, source_uri):
    external_config = bigquery.ExternalConfig("PARQUET")
    external_config.source_uris = [source_uri]
    table = bigquery.Table(table_name)
    table.external_data_configuration = external_config
    client.create_table(table, exists_ok=True)


def create_table(client, table_name, source_schema_table_name):
    table = bigquery.Table(table_name)
    table.schema = client.get_table(source_schema_table_name).schema
    table = client.create_table(table, exists_ok=True)


def merge_table(client, source, target, key_column):
    query_job = client.query(f"""
        merge into `{target}` as t
        using (select * from `{source}`) as s
        on t.{key_column} = s.{key_column}
        when not matched then insert row""")
    query_job.result()


def upload_taxi_data(year, month, bucket_uri):
    file = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
    gs_uri = f"{bucket_uri}/yellow_tripdata_{year}-{month:02d}.parquet"
    df = pl.read_parquet(file).with_columns(
        [pl.concat_str(key_columns()).hash().cast(str).alias("unique_row_id")]
    )
    write_gcs(df, gs_uri)


def ingest_taxi_data(year, month, project, dataset, bucket_uri):
    client = bigquery.Client()
    client.create_dataset(client.dataset("deng"), exists_ok=True)

    external_table_name = (
        f"{project}.{dataset}.nyc_taxi_ext_{''.join(random.choices(string.ascii_lowercase, k=10))}"
    )
    gs_uri = f"{bucket_uri}/yellow_tripdata_{year}-{month:02d}.parquet"
    create_external_table_from_parquet(client, external_table_name, gs_uri)

    table_name = f"{project}.{dataset}.nyc_taxi"
    create_table(client, table_name, external_table_name)

    merge_table(client, external_table_name, table_name, "unique_row_id")

    client.delete_table(external_table_name)
