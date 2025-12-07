from dagster import Definitions

from workflows.configs import init_io_manager, init_stdout_logger

from workflows.tests.assets import printed_logs, failed_asset
from workflows.tests.jobs import print_logs, print_logs_asset, fail_job, failed_job_asset

from workflows.numbers.jobs import process_numbers, process_numbers_k8s, process_numbers_k8s_celery, process_numbers_asset, process_numbers_asset_k8s, process_numbers_asset_k8s_celery
from workflows.numbers.assets import generated_numbers, filtered_even_numbers, filtered_odd_numbers, summed_even_numbers, summed_odd_numbers, summed_two_sums
from workflows.numbers.schedules import schedule_numbers_every_minute, schedule_numbers_k8s_every_minute, schedule_numbers_celery_every_minute, schedule_numbers_asset_every_minute, schedule_numbers_asset_k8s_every_minute, schedule_numbers_asset_k8s_celery_every_minute

from workflows.words.jobs import process_words_asset, process_words_asset_k8s, process_words_echo_external_k8s_job
from workflows.words.assets import generated_fruits_words, generated_animals_words, summed_words
from workflows.words.schedules import schedule_words_asset_every_minute, schedule_words_asset_k8s_every_minute

from workflows.weather.assets import fetched_southkorea_weather_hourly_csv, transformed_southkorea_weather_hourly_parquet, transformed_southkorea_weather_hourly_iceberg_parquet, transformed_southkorea_weather_daily_csv, transformed_southkorea_weather_daily_parquet, transformed_southkorea_weather_daily_iceberg_parquet, calculated_southkorea_weather_daily_average_parquet, calculated_southkorea_weather_daily_average_iceberg_parquet
from workflows.weather.jobs import process_weather_southkorea_hourly, process_weather_southkorea_daily
from workflows.weather.schedules import schedule_weather_southkorea_hourly, schedule_weather_southkorea_daily

defs = Definitions(
    assets=[
        # Tests
        printed_logs,
        failed_asset,

        # Numbers
        generated_numbers,
        filtered_even_numbers,
        filtered_odd_numbers,
        summed_even_numbers,
        summed_odd_numbers,
        summed_two_sums,

        # Words
        generated_fruits_words,
        generated_animals_words,
        summed_words,

        # Weather
        fetched_southkorea_weather_hourly_csv,
        transformed_southkorea_weather_hourly_parquet,
        transformed_southkorea_weather_hourly_iceberg_parquet,
        transformed_southkorea_weather_daily_csv,
        transformed_southkorea_weather_daily_parquet,
        transformed_southkorea_weather_daily_iceberg_parquet,
        calculated_southkorea_weather_daily_average_parquet,
        calculated_southkorea_weather_daily_average_iceberg_parquet,
    ],
    jobs=[
        # Tests
        print_logs,
        print_logs_asset,
        fail_job,
        failed_job_asset,

        # Numbers
        process_numbers,
        process_numbers_k8s,
        process_numbers_k8s_celery,
        process_numbers_asset,
        process_numbers_asset_k8s,
        process_numbers_asset_k8s_celery,

        # Words
        process_words_asset,
        process_words_asset_k8s,
        process_words_echo_external_k8s_job,

        # Weather
        process_weather_southkorea_hourly,
        process_weather_southkorea_daily,
    ],
    schedules=[
        # Numbers
        schedule_numbers_every_minute,
        schedule_numbers_k8s_every_minute,
        schedule_numbers_celery_every_minute,
        schedule_numbers_asset_every_minute,
        schedule_numbers_asset_k8s_every_minute,
        schedule_numbers_asset_k8s_celery_every_minute,

        # Words
        schedule_words_asset_every_minute,
        schedule_words_asset_k8s_every_minute,

        # Weather
        schedule_weather_southkorea_hourly,
        schedule_weather_southkorea_daily,
    ],
    resources=init_io_manager(),
    loggers={"console": init_stdout_logger},
)