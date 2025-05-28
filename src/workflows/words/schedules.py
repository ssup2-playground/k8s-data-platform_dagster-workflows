from dagster import ScheduleDefinition

from workflows.words.jobs import process_words_asset, process_words_asset_k8s

schedule_words_asset_every_minute = ScheduleDefinition(
    job=process_words_asset,
    cron_schedule="* * * * *",  # Run every minute
)

schedule_words_asset_k8s_every_minute = ScheduleDefinition(
    job=process_words_asset_k8s,
    cron_schedule="* * * * *",  # Run every minute
)