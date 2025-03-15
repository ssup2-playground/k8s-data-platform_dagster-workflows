from dagster import ScheduleDefinition

from workflows.numbers.jobs import process_numbers, process_numbers_k8s, process_numbers_asset, process_numbers_asset_k8s

process_numbers_every_minute = ScheduleDefinition(
    job=process_numbers,
    cron_schedule="* * * * *",  # Run every minute
)

process_numbers_k8s_every_minute = ScheduleDefinition(
    job=process_numbers_k8s,
    cron_schedule="* * * * *",  # Run every minute
)

process_numbers_asset_every_minute = ScheduleDefinition(
    job=process_numbers_asset,
    cron_schedule="* * * * *",  # Run every minute
)

process_numbers_asset_k8s_every_minute = ScheduleDefinition(
    job=process_numbers_asset_k8s,
    cron_schedule="* * * * *",  # Run every minute
)