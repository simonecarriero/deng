import os
import sys

import polars as pl


def ingest(conn, table_name, file):
    df = read(file)
    for chunk in df.iter_slices(n_rows=100_000):
        print(f"Inserting chunk: {len(chunk)}")
        save(chunk, conn, table_name)
    print("Done")


def read(file):
    if ".csv" in file:
        return pl.read_csv(file)
    elif ".parquet" in file:
        return pl.read_parquet(file)
    else:
        sys.exit("File type not supported, only csv and parquet files allowed")


def save(df, conn, table_name):
    return df.write_database(table_name=table_name, connection=conn, if_table_exists="append")


if __name__ == "__main__":
    username = os.environ["DB_USERNAME"]
    password = os.environ["DB_PASSWORD"]
    host = os.environ["DB_HOST"]
    port = os.environ["DB_PORT"]
    database = os.environ["DB_DATABASE"]
    table_name = os.environ["TABLE_NAME"]
    file = os.environ["FILE"]
    conn = f"postgresql://{username}:{password}@{host}:{port}/{database}"
    ingest(conn, table_name, file)
