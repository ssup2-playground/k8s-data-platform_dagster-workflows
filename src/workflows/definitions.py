from dagster import Definitions

from workflows.configs import get_io_manager

from workflows.numbers.jobs import process_numbers, process_numbers_k8s, process_numbers_k8s_celery, process_numbers_asset, process_numbers_asset_k8s, process_numbers_asset_k8s_celery
from workflows.numbers.assets import generated_numbers, filtered_even_numbers, filtered_odd_numbers, summed_even_numbers, summed_odd_numbers, summed_two_sums
from workflows.numbers.schedules import process_numbers_every_minute, process_numbers_k8s_every_minute, process_numbers_celery_every_minute, process_numbers_asset_every_minute, process_numbers_asset_k8s_every_minute, process_numbers_asset_k8s_celery_every_minute

from workflows.words.jobs import process_words_asset, process_words_asset_k8s, process_words_echo_external_k8s_job
from workflows.words.assets import generated_fruits_words, generated_animals_words, summed_words
from workflows.words.schedules import process_words_asset_every_minute, process_words_asset_k8s_every_minute

from workflows.weather.assets import fetched_southkorea_weather_data
from workflows.weather.partitions import hourly_southkorea_weather_partitions

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
        fetched_southkorea_weather_data,
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
    ],
    resources=get_io_manager(),
)