import io
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.table import Table

from dagster import asset, AssetExecutionContext

from workflows.configs import get_southkorea_weather_api_key, init_minio_client, get_iceberg_catalog, HIVE_CATALOG_URI
from workflows.weather.partitions import hourly_southkorea_weather_partitions, daily_southkorea_weather_partitions

from utils.southkorea import get_southkorea_weather_data

## Constants
MINIO_BUCKET = "weather"
MINIO_DIRECTORY_SOUTHKOREA_HOURLY_CSV = "southkorea/hourly-csv"
MINIO_DIRECTORY_SOUTHKOREA_HOURLY_PARQUET = "southkorea/hourly-parquet"
MINIO_DIRECTORY_SOUTHKOREA_HOURLY_ICEBERG_PARQUET = "southkorea/hourly-iceberg-parquet"
MINIO_DIRECTORY_SOUTHKOREA_DAILY_CSV = "southkorea/daily-csv"
MINIO_DIRECTORY_SOUTHKOREA_DAILY_PARQUET = "southkorea/daily-parquet"

ICEBERG_TABLE_HOURLY = "weather.southkorea_hourly_iceberg_parquet"
ICEBERG_TABLE_DAILY = "weather.southkorea_daily_iceberg_parquet"

ICEBERG_DAILY_AVERAGE_PYICEBERG_TABLE = "weather.southkorea_daily_average_iceberg_parquet"
ICEBERG_DAILY_AVERAGE_SPARK_TABLE = "iceberg.weather.southkorea_daily_average_iceberg_parquet"
ICEBERG_DAILY_SPARK_TABLE = "iceberg.weather.southkorea_daily_iceberg_parquet"

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
    tags={"schedule": "daily"},
    required_resource_keys={"pyspark"}
)
def calculated_southkorea_weather_daily_average_parquet(context: AssetExecutionContext):
    # Get date from partition key
    partition_date = context.partition_key
    dt = datetime.strptime(partition_date, "%Y-%m-%d")
    request_date = dt.strftime("%Y%m%d")
    year = int(request_date[0:4])
    month = int(request_date[4:6])
    day = int(request_date[6:8])

    # Check if average data exists in MinIO
    minio_client = init_minio_client()
    daily_average_parquet_path = (
        f"southkorea/daily-average-parquet/"
        f"year={year}/"
        f"month={month}/"
        f"day={day}"
    )
    try:
        minio_client.stat_object(MINIO_BUCKET, f"{daily_average_parquet_path}/_SUCCESS")
        context.log.info("Data already exists in MinIO")
        return 0
    except Exception as e:
        if "NoSuchKey" not in str(e):
            context.log.error(f"Unexpected error: {e}")
            return 1

    # Get Spark session from PySparkResource
    spark = context.resources.pyspark.spark_session
    spark.sparkContext.setLogLevel("INFO")
    
    # Read data from parquet
    daily_parquet_name = get_daily_parquet_object_name(request_date)
    df = spark.read.parquet(f"s3a://{MINIO_BUCKET}/{daily_parquet_name}")
    
    # Create temporary view
    TEMP_PARQUET_TABLE = "weather_southkorea_daily_parquet"
    df.createOrReplaceTempView(TEMP_PARQUET_TABLE)

    # Calculate average
    query = f"""
    SELECT
        branch_name,
        AVG(temp) as avg_temp,
        AVG(rain) as avg_rain,
        AVG(snow) as avg_snow,
        AVG(cloud_cover_total) as avg_cloud_cover_total,
        AVG(cloud_cover_lowmiddle) as avg_cloud_cover_lowmiddle,
        AVG(cloud_lowest) as avg_cloud_lowest,
        AVG(humidity) as avg_humidity,
        AVG(wind_speed) as avg_wind_speed,
        AVG(pressure_local) as avg_pressure_local,
        AVG(pressure_sea) as avg_pressure_sea,
        AVG(pressure_vaper) as avg_pressure_vaper,
        AVG(dew_point) as avg_dew_point
    FROM {TEMP_PARQUET_TABLE}
    GROUP BY branch_name
    """
    
    result_df = spark.sql(query)
    
    # Display results
    result_df.show(truncate=False)
    
    # Save results to MinIO
    result_df.write \
        .format("parquet") \
        .option("compression", "none") \
        .option("path", f"s3a://{MINIO_BUCKET}/{daily_average_parquet_path}") \
        .mode("overwrite") \
        .save()
    
    context.log.info(f"Successfully saved daily average data to {daily_average_parquet_path}")

@asset(
    key_prefix=["weather"],
    group_name="weather",
    description="Calculated daily south korea weather average data in Iceberg Parquet format",
    deps=[transformed_southkorea_weather_daily_iceberg_parquet],
    partitions_def=daily_southkorea_weather_partitions,
    kinds=["python"],
    tags={"schedule": "daily"},
    required_resource_keys={"pyspark"}
)
def calculated_southkorea_weather_daily_average_iceberg_parquet(context: AssetExecutionContext):
    # Get date from partition key
    partition_date = context.partition_key
    dt = datetime.strptime(partition_date, "%Y-%m-%d")
    request_date = dt.strftime("%Y%m%d")
    year = int(request_date[0:4])
    month = int(request_date[4:6])
    day = int(request_date[6:8])

    # Get Iceberg catalog and check if data exists
    catalog = get_iceberg_catalog()
    
    try:
        daily_average_table = catalog.load_table(ICEBERG_DAILY_AVERAGE_PYICEBERG_TABLE)
        if check_partition_exists_by_date(daily_average_table, request_date):
            context.log.info(f"Data already exists in Iceberg table for date {request_date}")
            return 0
    except Exception as e:
        context.log.warning(f"Could not check Iceberg table: {e}")

    # Get Spark session from PySparkResource
    spark = context.resources.pyspark.spark_session
    spark.sparkContext.setLogLevel("INFO")
    
    # Configure Spark for Iceberg (additional settings beyond common config)
    spark.conf.set("hive.metastore.uris", HIVE_CATALOG_URI)
    spark.conf.set("spark.sql.catalog.uri", HIVE_CATALOG_URI)
    spark.conf.set("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.iceberg.type", "hive")
    
    # Calculate average from Iceberg table
    avg_query = f"""
    SELECT
        branch_name,
        AVG(temp) as avg_temp,
        AVG(rain) as avg_rain,
        AVG(snow) as avg_snow,
        AVG(cloud_cover_total) as avg_cloud_cover_total,
        AVG(cloud_cover_lowmiddle) as avg_cloud_cover_lowmiddle,
        AVG(cloud_lowest) as avg_cloud_lowest,
        AVG(humidity) as avg_humidity,
        AVG(wind_speed) as avg_wind_speed,
        AVG(pressure_local) as avg_pressure_local,
        AVG(pressure_sea) as avg_pressure_sea,
        AVG(pressure_vaper) as avg_pressure_vaper,
        AVG(dew_point) as avg_dew_point,
        year,
        month,
        day
    FROM {ICEBERG_DAILY_SPARK_TABLE}
    WHERE year = {year} AND month = {month} AND day = {day}
    GROUP BY branch_name, year, month, day
    """
    
    result_df = spark.sql(avg_query)
    
    # Display results
    result_df.show(truncate=False)
    
    # Save results to Iceberg table
    result_df.write \
        .format("iceberg") \
        .mode("append") \
        .partitionBy("year", "month", "day") \
        .saveAsTable(ICEBERG_DAILY_AVERAGE_SPARK_TABLE)
    
    context.log.info(f"Successfully saved daily average data to Iceberg table {ICEBERG_DAILY_AVERAGE_SPARK_TABLE}")