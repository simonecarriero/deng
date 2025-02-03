import random
import string
import sys
import polars as pl
import sqlalchemy


def read_file(source):
    if ".csv" in source:
        return pl.read_csv(source)
    elif ".parquet" in source:
        return pl.read_parquet(source)
    else:
        sys.exit("File type not supported, only csv and parquet files allowed")


def write_db(df, conn, table_name):
    for chunk in df.iter_slices(n_rows=100_000):
        print(f"Inserting chunk: {len(chunk)}")
        chunk.write_database(table_name=table_name, connection=conn, if_table_exists="append")


def key_columns(taxi_type):
    pep = "tpep" if taxi_type == "yellow" else "lpep"
    return [
        "VendorID",
        f"{pep}_pickup_datetime",
        f"{pep}_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "fare_amount",
        "trip_distance",
    ]


def merge_table(conn, source, target, key_column):
    conn.execute(
        sqlalchemy.text(f"""
        create table if not exists {target} (like {source} including all);
        merge into {target} as t
        using {source} as s on t."{key_column}" = s."{key_column}"
        when not matched then insert values (s.*);
        drop table {source};
    """)
    )
    conn.commit()


def ingest_taxi_db(conn_uri, taxi_type, file):
    df = read_file(file).with_columns(
        [pl.concat_str(key_columns(taxi_type)).hash().cast(str).alias("unique_row_id")]
    )
    with sqlalchemy.create_engine(conn_uri).connect() as conn:
        table = f"ny_taxi_data_{taxi_type}"
        staging_table = f"{table}_{''.join(random.choices(string.ascii_lowercase, k=10))}"
        write_db(df, conn, staging_table)
        merge_table(conn, staging_table, table, "unique_row_id")


def ingest_zones_db(conn_uri, file):
    df = read_file(file)
    write_db(df, conn_uri, "taxi_zone_lookup")
