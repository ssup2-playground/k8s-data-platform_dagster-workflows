import io
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from dagster import asset, AssetExecutionContext, Nothing

from workflows.configs import get_southkorea_weather_api_key, get_minio_client, get_iceberg_catalog
from workflows.weather.partitions import hourly_southkorea_weather_partitions
from weather.iceberg import retry_append_table
from weather.southkorea import get_southkorea_weather_data

## Constants
MINIO_BUCKET = "weather"
MINIO_DIRECTORY_SOUTHKOREA_HOURLY_CSV = "southkorea/hourly-csv"
MINIO_DIRECTORY_SOUTHKOREA_HOURLY_PARQUET = "southkorea/hourly-parquet"
MINIO_DIRECTORY_SOUTHKOREA_HOURLY_ICEBERG_PARQUET = "southkorea/hourly-iceberg-parquet"

ICEBERG_TABLE = "weather.southkorea_hourly_iceberg_parquet"

## Functions
def get_partition_datetime(partition_key: str) -> datetime:
    '''Convert partition key to datetime object'''
    return datetime.strptime(partition_key, "%Y-%m-%d-%H:%M")

def get_hourly_csv_object_name(dt: datetime) -> str:
    '''Get object name'''
    return (
        f"{MINIO_DIRECTORY_SOUTHKOREA_HOURLY_CSV}/"
        f"year={dt.strftime('%Y')}/"
        f"month={dt.strftime('%m')}/"
        f"day={dt.strftime('%d')}/"
        f"hour={dt.strftime('%H')}/"
        f"data.csv"
    )

def get_hourly_parquet_object_name(dt: datetime) -> str:
    '''Get object name'''
    return (
        f"{MINIO_DIRECTORY_SOUTHKOREA_HOURLY_PARQUET}/"
        f"year={dt.strftime('%Y')}/"
        f"month={dt.strftime('%m')}/"
        f"day={dt.strftime('%d')}/"
        f"hour={dt.strftime('%H')}/"
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
    # Init variables
    minio_client = get_minio_client()
    dt = get_partition_datetime(context.partition_key)
    
    # Check if data exists in MinIO
    object_csv_name = get_hourly_csv_object_name(dt)
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
    data = get_southkorea_weather_data(api_key, dt)

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
    # Init variables
    minio_client = get_minio_client()
    dt = get_partition_datetime(context.partition_key)

    # Check if data exists in MinIO
    object_parquet_name = get_hourly_parquet_object_name(dt)
    try:
        minio_client.stat_object(MINIO_BUCKET, object_parquet_name)
        print("data already exists in minio")
        return 0
    except Exception as e:
        if "NoSuchKey" not in str(e):
            print("Unexpected error : {0}".format(e))
            return 1

    # Get CSV data
    object_csv_name = get_hourly_csv_object_name(dt)
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
    # Init variables
    minio_client = get_minio_client()
    dt = get_partition_datetime(context.partition_key)
    
    # Get Iceberg table
    catalog = get_iceberg_catalog()
    iceberg_table = catalog.load_table(ICEBERG_TABLE)

    # Check if partition data exists
    partition_count = iceberg_table.scan(
        row_filter=f"year = {dt.year} AND month = {dt.month} AND day = {dt.day} AND hour = {dt.hour}"
    ).to_arrow().num_rows
    if partition_count > 0:
        context.log.info(f"Data for partition year={dt.year}, month={dt.month}, day={dt.day}, hour={dt.hour} already exists. Skipping insert.")
        return

    # Get Parquet data and convert directly to PyArrow table
    object_parquet_name = get_hourly_parquet_object_name(dt)
    parquet_data = minio_client.get_object(bucket_name=MINIO_BUCKET,
                                         object_name=object_parquet_name)
    
    # Read Parquet data as PyArrow table
    buffer = io.BytesIO(parquet_data.read())
    buffer.seek(0)
    table = pq.read_table(buffer)
    
    # Convert types to match Iceberg schema
    table = table.cast(pa.schema([
        ('branch_name', pa.string()),
        ('temp', pa.float64()),
        ('rain', pa.float64()),
        ('snow', pa.float64()),
        ('cloud_cover_total', pa.int32()),
        ('cloud_cover_lowmiddle', pa.int32()),
        ('cloud_lowest', pa.int32()),
        ('cloud_shape', pa.string()),
        ('humidity', pa.int32()),
        ('wind_speed', pa.float64()),
        ('wind_direction', pa.string()),
        ('pressure_local', pa.float64()),
        ('pressure_sea', pa.float64()),
        ('pressure_vaper', pa.float64()),
        ('dew_point', pa.float64()),
    ]))
    
    # Add partition columns with correct types
    table = table.append_column('year', pa.array([dt.year] * len(table), type=pa.int32()))
    table = table.append_column('month', pa.array([dt.month] * len(table), type=pa.int32()))
    table = table.append_column('day', pa.array([dt.day] * len(table), type=pa.int32()))
    table = table.append_column('hour', pa.array([dt.hour] * len(table), type=pa.int32()))

    # Append the data
    error = retry_append_table(iceberg_table, table)
    if error:
        context.log.error(f"Failed to insert data for partition year={dt.year}, month={dt.month}, day={dt.day}, hour={dt.hour}")
        raise error
