import io
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from dagster import asset, AssetExecutionContext

from workflows.configs import get_southkorea_weather_api_key, get_minio_client
from workflows.weather.partitions import hourly_southkorea_weather_partitions
from weather.southkorea import get_southkorea_weather_data

## Constants
MINIO_BUCKET = "weather"
MINIO_DIRECTORY_SOUTHKOREA_HOURLY_PARQUET = "southkorea/hourly-parquet"

## Functions
def get_object_name(directory: str, date: str, hour: str) -> str:
    '''Get object name'''
    return (
        f"{directory}/"
        f"year={date[0:4]}/"
        f"month={date[4:6]}/"
        f"day={date[6:8]}/"
        f"hour={hour.zfill(2)}/"
        f"data.parquet"
    )

## Assets
@asset(
    key_prefix=["examples"],
    group_name="weather",
    description="Fetched South Korea weather data",
    partitions_def=hourly_southkorea_weather_partitions,
    kinds=["python"],
)
def fetched_southkorea_weather_data(context: AssetExecutionContext):
    # Get MinIO Client
    minio_client = get_minio_client()
    api_key = get_southkorea_weather_api_key()
    
    # Get Date and Hour
    partition_date_hour = context.partition_key  # format: "2023-01-01-00:00"
    dt = datetime.strptime(partition_date_hour, "%Y-%m-%d-%H:%M")
    request_date = dt.strftime("%Y%m%d")
    request_hour = dt.strftime("%H")

    # Get Object Name
    object_name = get_object_name(MINIO_DIRECTORY_SOUTHKOREA_HOURLY_PARQUET, request_date, request_hour)

    # Check if data exists in MinIO
    try:
        minio_client.stat_object(MINIO_BUCKET, object_name)
        print("data already exists in minio") 
        return 0
    except Exception as e:
        if "NoSuchKey" not in str(e):
            print("Unexpected error : {0}".format(e))
            return 1

    # Get data
    data = get_southkorea_weather_data(api_key, request_date, request_hour)

    # Convert to Parquet
    dataframe = pd.DataFrame(data)
    table = pa.Table.from_pandas(dataframe)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    # Write to MinIO
    minio_client.put_object(bucket_name=MINIO_BUCKET,
                            object_name=object_name,
                            data=buffer,
                            length=buffer.getbuffer().nbytes)
