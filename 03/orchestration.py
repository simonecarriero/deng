import os
import dagster as dg
from datetime import datetime
import nyc_taxi

partitions = dg.MonthlyPartitionsDefinition(start_date="2019-01-01")


@dg.asset(partitions_def=partitions)
def upload_taxi_data(context):
    date = datetime.strptime(context.partition_key, "%Y-%m-%d")
    nyc_taxi.upload_taxi_data(date.year, date.month, os.environ["GOOGLE_BUCKET"])


@dg.asset(
    partitions_def=partitions,
    deps=[upload_taxi_data],
    automation_condition=dg.AutomationCondition.on_missing(),
)
def ingest_taxi_db(context):
    date = datetime.strptime(context.partition_key, "%Y-%m-%d")

    nyc_taxi.ingest_taxi_data(
        date.year, date.month, os.environ["GOOGLE_PROJECT"], "deng", os.environ["GOOGLE_BUCKET"]
    )


defs = dg.Definitions(
    assets=[
        upload_taxi_data,
        ingest_taxi_db,
    ]
)
