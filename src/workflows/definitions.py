from dagster import Definitions

from workflows.configs import init_io_manager

from workflows.numbers.jobs import process_numbers, process_numbers_k8s, process_numbers_k8s_celery, process_numbers_asset, process_numbers_asset_k8s, process_numbers_asset_k8s_celery
from workflows.numbers.assets import generated_numbers, filtered_even_numbers, filtered_odd_numbers, summed_even_numbers, summed_odd_numbers, summed_two_sums
from workflows.numbers.schedules import process_numbers_every_minute, process_numbers_k8s_every_minute, process_numbers_celery_every_minute, process_numbers_asset_every_minute, process_numbers_asset_k8s_every_minute, process_numbers_asset_k8s_celery_every_minute

from workflows.words.jobs import process_words_asset, process_words_asset_k8s, process_words_echo_external_k8s_job
from workflows.words.assets import generated_fruits_words, generated_animals_words, summed_words
from workflows.words.schedules import process_words_asset_every_minute, process_words_asset_k8s_every_minute

from workflows.weather.assets import fetched_southkorea_weather_csv_data, transformed_southkorea_weather_parquet_data, transformed_southkorea_weather_iceberg_parquet_data
from workflows.weather.jobs import process_weather_southkorea
from workflows.weather.schedules import process_weather_southkorea_every_hour

defs = Definitions(
    assets=[
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
        fetched_southkorea_weather_csv_data,
        transformed_southkorea_weather_parquet_data,
        transformed_southkorea_weather_iceberg_parquet_data,
    ],
    jobs=[
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
        process_weather_southkorea,
    ],
    schedules=[
        # Numbers
        process_numbers_every_minute,
        process_numbers_k8s_every_minute,
        process_numbers_celery_every_minute,
        process_numbers_asset_every_minute,
        process_numbers_asset_k8s_every_minute,
        process_numbers_asset_k8s_celery_every_minute,

        # Words
        process_words_asset_every_minute,
        process_words_asset_k8s_every_minute,

        # Weather
        process_weather_southkorea_every_hour,
    ],
    resources=init_io_manager(),
)