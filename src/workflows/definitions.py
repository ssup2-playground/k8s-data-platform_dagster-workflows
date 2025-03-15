from dagster import Definitions

from workflows.configs import get_io_manager
from workflows.numbers.jobs import process_numbers, process_numbers_k8s, process_numbers_asset, process_numbers_asset_k8s
from workflows.numbers.assets import generated_numbers, filtered_even_numbers, filtered_odd_numbers, summed_even_numbers, summed_odd_numbers, summed_two_numbers
from workflows.numbers.schedules import process_numbers_every_minute, process_numbers_k8s_every_minute, process_numbers_asset_every_minute, process_numbers_asset_k8s_every_minute
from workflows.words.jobs import process_words_asset, process_words_asset_k8s
from workflows.words.assets import generated_fruits_words, generated_animals_words, summed_words
from workflows.words.schedules import process_words_asset_every_minute, process_words_asset_k8s_every_minute

defs = Definitions(
    assets=[
        # Numbers
        generated_numbers,
        filtered_even_numbers,
        filtered_odd_numbers,
        summed_even_numbers,
        summed_odd_numbers,
        summed_two_numbers,

        # Words
        generated_fruits_words,
        generated_animals_words,
        summed_words,
    ],
    jobs=[
        # Numbers
        process_numbers,
        process_numbers_k8s,
        process_numbers_asset,
        process_numbers_asset_k8s,

        # Words
        process_words_asset,
        process_words_asset_k8s,
    ],
    schedules=[
        # Numbers
        process_numbers_every_minute,
        process_numbers_k8s_every_minute,
        process_numbers_asset_every_minute,
        process_numbers_asset_k8s_every_minute,

        # Words
        process_words_asset_every_minute,
        process_words_asset_k8s_every_minute,
    ],
    resources=get_io_manager(),
)