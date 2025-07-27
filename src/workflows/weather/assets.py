import io
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.table import Table

from dagster import asset, AssetExecutionContext

from workflows.configs import get_southkorea_weather_api_key, init_minio_client, get_iceberg_catalog
from workflows.weather.partitions import hourly_southkorea_weather_partitions, daily_southkorea_weather_partitions

from utils.southkorea import get_southkorea_weather_data
from utils.spark import execute_spark_job

## Constants
MINIO_BUCKET = "weather"
MINIO_DIRECTORY_SOUTHKOREA_HOURLY_CSV = "southkorea/hourly-csv"
MINIO_DIRECTORY_SOUTHKOREA_HOURLY_PARQUET = "southkorea/hourly-parquet"
MINIO_DIRECTORY_SOUTHKOREA_HOURLY_ICEBERG_PARQUET = "southkorea/hourly-iceberg-parquet"
MINIO_DIRECTORY_SOUTHKOREA_DAILY_CSV = "southkorea/daily-csv"
MINIO_DIRECTORY_SOUTHKOREA_DAILY_PARQUET = "southkorea/daily-parquet"

ICEBERG_TABLE_HOURLY = "weather.southkorea_hourly_iceberg_parquet"
ICEBERG_TABLE_DAILY = "weather.southkorea_daily_iceberg_parquet"

## Functions
def get_hourly_csv_object_name(date: str, hour: str) -> str:
    '''Get hourly csv object name'''
    return (
        f"{MINIO_DIRECTORY_SOUTHKOREA_HOURLY_CSV}/"
        f"year={int(date[0:4])}/"
        f"month={int(date[4:6])}/"
        f"day={int(date[6:8])}/"
        f"hour={int(hour)}/"
        f"data.csv"
    )

def get_hourly_parquet_object_name(date: str, hour: str) -> str:
    '''Get hourly parquet object name'''
    return (
        f"{MINIO_DIRECTORY_SOUTHKOREA_HOURLY_PARQUET}/"
        f"year={int(date[0:4])}/"
        f"month={int(date[4:6])}/"
        f"day={int(date[6:8])}/"
        f"hour={int(hour)}/"
        f"data.parquet"
    )

def get_daily_csv_object_name(date: str) -> str:
    '''Get daily csv object name'''
    return (
        f"{MINIO_DIRECTORY_SOUTHKOREA_DAILY_CSV}/"
        f"year={int(date[0:4])}/"
        f"month={int(date[4:6])}/"
        f"day={int(date[6:8])}/"
        f"data.csv"
    )

def get_daily_parquet_object_name(date: str) -> str:
    '''Get daily parquet object name'''
    return (
        f"{MINIO_DIRECTORY_SOUTHKOREA_DAILY_PARQUET}/"
        f"year={int(date[0:4])}/"
        f"month={int(date[4:6])}/"
        f"day={int(date[6:8])}/"
        f"data.parquet"
    )

def check_partition_exists_by_date_and_hour(iceberg_table: Table, date: str, hour: str) -> bool:
    '''Check if a specific partition exists using inspect.partitions()'''
    year = int(date[0:4])
    month = int(date[4:6])
    day = int(date[6:8])

    date_list = iceberg_table.inspect.partitions()["partition"].to_pylist()
    date_set = set(tuple(date.values()) for date in date_list)
    return (year, month, day, int(hour)) in date_set

def check_partition_exists_by_date(iceberg_table: Table, date: str) -> bool:
    '''Check if a specific partition exists using inspect.partitions()'''
    year = int(date[0:4])
    month = int(date[4:6])
    day = int(date[6:8])

    date_list = iceberg_table.inspect.partitions()["partition"].to_pylist()
    date_set = set(tuple(date.values()) for date in date_list)
    return (year, month, day) in date_set

## Assets
@asset(
    key_prefix=["weather"],
    group_name="weather",
    description="Fetched hourly south korea weather data in CSV format",
    partitions_def=hourly_southkorea_weather_partitions,
    kinds=["python"],
    tags={"schedule": "hourly"}
)
def fetched_southkorea_weather_hourly_csv(context: AssetExecutionContext):
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

    # Convert to CSV
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
    key_prefix=["weather"],
    group_name="weather",
    description="Transformed hourly south korea weather data in Parquet format",
    deps=[fetched_southkorea_weather_hourly_csv],
    partitions_def=hourly_southkorea_weather_partitions,
    kinds=["python"],
    tags={"schedule": "hourly"}
)
def transformed_southkorea_weather_hourly_parquet(context: AssetExecutionContext):
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
    key_prefix=["weather"],
    group_name="weather",
    description="Transform hourly parquet data to Iceberg table",
    deps=[transformed_southkorea_weather_hourly_parquet],
    partitions_def=hourly_southkorea_weather_partitions,
    kinds=["python"],
    tags={"schedule": "hourly"}
)
def transformed_southkorea_weather_hourly_iceberg_parquet(context: AssetExecutionContext):
    # Init MinIO client
    minio_client = init_minio_client()
    
    # Get date and hour from partition key
    partition_date_hour = context.partition_key
    dt = datetime.strptime(partition_date_hour, "%Y-%m-%d-%H:%M")
    request_date = dt.strftime("%Y%m%d")
    request_hour = dt.strftime("%H")

    # Get Iceberg table
    catalog = get_iceberg_catalog()
    iceberg_table = catalog.load_table(ICEBERG_TABLE_HOURLY)

    # Check if partition exists
    if check_partition_exists_by_date_and_hour(iceberg_table, request_date, request_hour):
        context.log.info(f"Data already exists in Iceberg table for {partition_date_hour}")
        return 0

    # Get data and convert directly to PyArrow Table
    object_parquet_name = get_hourly_parquet_object_name(request_date, request_hour)
    parquet_data = minio_client.get_object(bucket_name=MINIO_BUCKET,
                                         object_name=object_parquet_name)
    
    # Read directly as PyArrow Table
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
    table = table.append_column('year', pa.array([int(request_date[0:4])] * len(table), type=pa.int32()))
    table = table.append_column('month', pa.array([int(request_date[4:6])] * len(table), type=pa.int32()))
    table = table.append_column('day', pa.array([int(request_date[6:8])] * len(table), type=pa.int32()))
    table = table.append_column('hour', pa.array([int(request_hour.zfill(2))] * len(table), type=pa.int32()))

    # Load Iceberg table and append data
    catalog = get_iceberg_catalog()
    iceberg_table = catalog.load_table(ICEBERG_TABLE_HOURLY)
    iceberg_table.append(table)

@asset(
    key_prefix=["weather"],
    group_name="weather",
    description="Transformed daily south korea weather data in CSV format",
    deps=[fetched_southkorea_weather_hourly_csv],
    partitions_def=daily_southkorea_weather_partitions,
    kinds=["python"],
    tags={"schedule": "daily"}
)
def transformed_southkorea_weather_daily_csv(context: AssetExecutionContext):
    # Init MinIO client
    minio_client = init_minio_client()
    
    # Get date from partition key
    partition_date = context.partition_key
    dt = datetime.strptime(partition_date, "%Y-%m-%d")
    request_date = dt.strftime("%Y%m%d")
    
    # Check if daily data already exists
    daily_csv_name = get_daily_csv_object_name(request_date)
    try:
        minio_client.stat_object(MINIO_BUCKET, daily_csv_name)
        context.log.info(f"Daily data already exists for date {request_date}")
        return 0
    except Exception as e:
        if "NoSuchKey" not in str(e):
            context.log.error(f"Unexpected error: {e}")
            return 1
    
    # Get all hourly data for the day
    daily_data = []
    for hour in range(24):
        hour_str = f"{hour:02d}"
        hourly_csv_name = get_hourly_csv_object_name(request_date, hour_str)
        
        try:
            csv_data = minio_client.get_object(bucket_name=MINIO_BUCKET,
                                             object_name=hourly_csv_name)
            hourly_df = pd.read_csv(csv_data)
            # Add hour column
            hourly_df['hour'] = int(hour_str)
            daily_data.append(hourly_df)
        except Exception as e:
            if "NoSuchKey" not in str(e):
                context.log.error(f"Error reading hourly data for hour {hour}: {e}")
            continue
    
    if not daily_data:
        context.log.info(f"No hourly data found for date {request_date}")
        return 0
    
    # Combine all hourly data
    daily_df = pd.concat(daily_data, ignore_index=True)
    daily_df = daily_df.sort_values(['branch_name', 'hour'])
    
    # Save as CSV
    buffer = io.BytesIO()
    daily_df.to_csv(buffer, index=False)
    buffer.seek(0)
    
    # Write to MinIO
    minio_client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=daily_csv_name,
        data=buffer,
        length=buffer.getbuffer().nbytes
    )

@asset(
    key_prefix=["weather"],
    group_name="weather",
    description="Transformed daily south korea weather data in Parquet format",
    deps=[transformed_southkorea_weather_daily_csv],
    partitions_def=daily_southkorea_weather_partitions,
    kinds=["python"],
    tags={"schedule": "daily"}
)
def transformed_southkorea_weather_daily_parquet(context: AssetExecutionContext):
    # Init MinIO client
    minio_client = init_minio_client()
    
    # Get date from partition key
    partition_date = context.partition_key
    dt = datetime.strptime(partition_date, "%Y-%m-%d")
    request_date = dt.strftime("%Y%m%d")
    
    # Check if daily data already exists
    daily_parquet_name = get_daily_parquet_object_name(request_date)
    try:
        minio_client.stat_object(MINIO_BUCKET, daily_parquet_name)
        context.log.info(f"Daily data already exists for date {request_date}")
        return 0
    except Exception as e:
        if "NoSuchKey" not in str(e):
            context.log.error(f"Unexpected error: {e}")
            return 1
    
    # Get data
    object_csv_name = get_daily_csv_object_name(request_date)
    csv_data = minio_client.get_object(bucket_name=MINIO_BUCKET,
                                      object_name=object_csv_name)
    dataframe = pd.read_csv(csv_data)
    
    # Convert to Parquet
    table = pa.Table.from_pandas(dataframe)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)
    
    # Write to MinIO
    minio_client.put_object(bucket_name=MINIO_BUCKET,
                            object_name=daily_parquet_name,
                            data=buffer,
                            length=buffer.getbuffer().nbytes)

@asset(
    key_prefix=["weather"],
    group_name="weather",
    description="Transformed daily south korea weather data in Iceberg Parquet format",
    deps=[transformed_southkorea_weather_daily_parquet],
    partitions_def=daily_southkorea_weather_partitions,
    kinds=["python"],
    tags={"schedule": "daily"}
)
def transformed_southkorea_weather_daily_iceberg_parquet(context: AssetExecutionContext):
    # Init MinIO client
    minio_client = init_minio_client()
    
    # Get date from partition key
    partition_date = context.partition_key
    dt = datetime.strptime(partition_date, "%Y-%m-%d")
    request_date = dt.strftime("%Y%m%d")

    # Get Iceberg table
    catalog = get_iceberg_catalog()
    iceberg_table = catalog.load_table(ICEBERG_TABLE_DAILY)

    # Check if partition exists
    if check_partition_exists_by_date(iceberg_table, request_date):
        context.log.info(f"Data already exists in Iceberg table for {partition_date}")
        return 0
    
    # Get data
    object_parquet_name = get_daily_parquet_object_name(request_date)
    parquet_data = minio_client.get_object(bucket_name=MINIO_BUCKET,
                                         object_name=object_parquet_name)
    
    # Read directly as PyArrow Table
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
        ('hour', pa.int32()),
    ]))
    
    # Add partition columns with correct types
    table = table.append_column('year', pa.array([int(request_date[0:4])] * len(table), type=pa.int32()))
    table = table.append_column('month', pa.array([int(request_date[4:6])] * len(table), type=pa.int32()))
    table = table.append_column('day', pa.array([int(request_date[6:8])] * len(table), type=pa.int32()))
    
    # Load Iceberg table and append data
    catalog = get_iceberg_catalog()
    iceberg_table = catalog.load_table(ICEBERG_TABLE_DAILY)
    iceberg_table.append(table)

@asset(
    key_prefix=["weather"],
    group_name="weather",
    description="Calculated daily south korea weather average data in Parquet format",
    deps=[transformed_southkorea_weather_daily_parquet],
    partitions_def=daily_southkorea_weather_partitions,
    kinds=["python"],
    tags={"schedule": "daily"}
)
def calculated_southkorea_weather_daily_average_parquet(context: AssetExecutionContext):
    # Get date from partition key
    partition_date = context.partition_key
    dt = datetime.strptime(partition_date, "%Y-%m-%d")
    request_date = dt.strftime("%Y%m%d")

    # Execute Spark job using abstracted function
    execute_spark_job(
        context=context,
        job_name_prefix="spark-daily-average-parquet-" + f"{request_date}",
        job_script="local:///app/jobs/weather_southkorea_daily_average_parquet.py",
        job_args=["--date", request_date],
        spark_image="ghcr.io/ssup2-playground/k8s-data-platform_spark-jobs:0.1.10",
        jars=["org.apache.hadoop:hadoop-aws:3.3.4", "com.amazonaws:aws-java-sdk-bundle:1.12.262"],
        timeout_seconds=600
    )

@asset(
    key_prefix=["weather"],
    group_name="weather",
    description="Calculated daily south korea weather average data in Iceberg Parquet format",
    deps=[transformed_southkorea_weather_daily_iceberg_parquet],
    partitions_def=daily_southkorea_weather_partitions,
    kinds=["python"],
    tags={"schedule": "daily"}
)
def calculated_southkorea_weather_daily_average_iceberg_parquet(context: AssetExecutionContext):
    # Get date from partition key
    partition_date = context.partition_key
    dt = datetime.strptime(partition_date, "%Y-%m-%d")
    request_date = dt.strftime("%Y%m%d")
    
    # Execute Spark job using abstracted function
    execute_spark_job(
        context=context,
        job_name_prefix="spark-daily-average-iceberg-parquet-" + f"{request_date}",
        job_script="local:///app/jobs/weather_southkorea_daily_average_iceberg_parquet.py",
        job_args=["--date", request_date],
        spark_image="ghcr.io/ssup2-playground/k8s-data-platform_spark-jobs:0.1.10",
        jars=["org.apache.hadoop:hadoop-aws:3.3.4", "com.amazonaws:aws-java-sdk-bundle:1.12.262", "org.apache.iceberg:iceberg-spark3-runtime:0.13.2"],
        timeout_seconds=600
    )