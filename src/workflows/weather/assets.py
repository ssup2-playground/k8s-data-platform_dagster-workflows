import io
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from dagster import asset, AssetExecutionContext, Nothing

from workflows.configs import get_southkorea_weather_api_key, init_minio_client, get_iceberg_catalog
from workflows.weather.partitions import hourly_southkorea_weather_partitions
from weather.southkorea import get_southkorea_weather_data

## Constants
MINIO_BUCKET = "weather"
MINIO_DIRECTORY_SOUTHKOREA_HOURLY_CSV = "southkorea/hourly-csv"
MINIO_DIRECTORY_SOUTHKOREA_HOURLY_PARQUET = "southkorea/hourly-parquet"
MINIO_DIRECTORY_SOUTHKOREA_HOURLY_ICEBERG_PARQUET = "southkorea/hourly-iceberg-parquet"

ICEBERG_TABLE = "weather.southkorea_hourly_iceberg_parquet"

## Functions
def get_hourly_csv_object_name(date: str, hour: str) -> str:
    '''Get object name'''
    return (
        f"{MINIO_DIRECTORY_SOUTHKOREA_HOURLY_CSV}/"
        f"year={date[0:4]}/"
        f"month={date[4:6]}/"
        f"day={date[6:8]}/"
        f"hour={hour.zfill(2)}/"
        f"data.csv"
    )

def get_hourly_parquet_object_name(date: str, hour: str) -> str:
    '''Get object name'''
    return (
        f"{MINIO_DIRECTORY_SOUTHKOREA_HOURLY_PARQUET}/"
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
    description="Fetched South Korea weather data in CSV format",
    partitions_def=hourly_southkorea_weather_partitions,
    kinds=["python"],
)
def fetched_southkorea_weather_csv_data(context: AssetExecutionContext):
    # Init MinIO client
    minio_client = init_minio_client()
    
    # Get date and hour
    partition_date_hour = context.partition_key  # format: "2023-01-01-00:00"
    dt = datetime.strptime(partition_date_hour, "%Y-%m-%d-%H:%M")
    request_date = dt.strftime("%Y%m%d")
    request_hour = dt.strftime("%H")

    # Get object name
    object_csv_name = get_hourly_csv_object_name(request_date, request_hour)

    # Check if data exists in MinIO
    try:
        minio_client.stat_object(MINIO_BUCKET, object_csv_name)
        print("data already exists in minio")
        return 0
    except Exception as e:
        if "NoSuchKey" not in str(e):
            print("Unexpected error : {0}".format(e))
            return 1

    # Get data
    api_key = get_southkorea_weather_api_key()
    data = get_southkorea_weather_data(api_key, request_date, request_hour)

    # Convert to Parquet
    dataframe = pd.DataFrame(data)
    buffer = io.BytesIO()
    dataframe.to_csv(buffer, index=False)
    buffer.seek(0)

    # Write to MinIO
    minio_client.put_object(bucket_name=MINIO_BUCKET,
                            object_name=object_csv_name,
                            data=buffer,
                            length=buffer.getbuffer().nbytes)

@asset(
    key_prefix=["examples"],
    group_name="weather",
    description="Fetched South Korea weather data in Parquet format",
    deps=[fetched_southkorea_weather_csv_data],
    partitions_def=hourly_southkorea_weather_partitions,
    kinds=["python"],
)
def transformed_southkorea_weather_parquet_data(context: AssetExecutionContext):
    # Init MinIO client
    minio_client = init_minio_client()
    
    # Get date and hour
    partition_date_hour = context.partition_key  # format: "2023-01-01-00:00"
    dt = datetime.strptime(partition_date_hour, "%Y-%m-%d-%H:%M")
    request_date = dt.strftime("%Y%m%d")
    request_hour = dt.strftime("%H")

    # Check if data exists in MinIO
    object_parquet_name = get_hourly_parquet_object_name(request_date, request_hour)
    try:
        minio_client.stat_object(MINIO_BUCKET, object_parquet_name)
        print("data already exists in minio")
        return 0
    except Exception as e:
        if "NoSuchKey" not in str(e):
            print("Unexpected error : {0}".format(e))
            return 1

    # Get data
    object_csv_name = get_hourly_csv_object_name(request_date, request_hour)
    csv_data = minio_client.get_object(bucket_name=MINIO_BUCKET,
                            object_name=object_csv_name)

    # Convert from CSV to Parquet
    dataframe = pd.read_csv(csv_data)
    table = pa.Table.from_pandas(dataframe)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    # Write to MinIO
    minio_client.put_object(bucket_name=MINIO_BUCKET,
                            object_name=object_parquet_name,
                            data=buffer,
                            length=buffer.getbuffer().nbytes)

@asset(
    key_prefix=["examples"],
    group_name="weather",
    description="Transform Parquet data to Iceberg table",
    deps=[transformed_southkorea_weather_parquet_data],
    partitions_def=hourly_southkorea_weather_partitions,
    kinds=["python"],
)
def transformed_southkorea_weather_iceberg_parquet_data(context: AssetExecutionContext):
    # Init MinIO client
    minio_client = init_minio_client()
    
    # Get date and hour
    partition_date_hour = context.partition_key
    dt = datetime.strptime(partition_date_hour, "%Y-%m-%d-%H:%M")
    request_date = dt.strftime("%Y%m%d")
    request_hour = dt.strftime("%H")

    # Get data
    object_parquet_name = get_hourly_parquet_object_name(request_date, request_hour)
    parquet_data = minio_client.get_object(bucket_name=MINIO_BUCKET,
                                         object_name=object_parquet_name)

    # Write to Iceberg table
    catalog = get_iceberg_catalog()
    table = catalog.load_table(ICEBERG_TABLE)
    df = pd.read_parquet(parquet_data)
    table.append(df)