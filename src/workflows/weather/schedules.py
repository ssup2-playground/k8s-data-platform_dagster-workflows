from dagster import ScheduleDefinition

from workflows.weather.jobs import process_weather_southkorea

process_weather_southkorea_every_hour = ScheduleDefinition(
    job=process_weather_southkorea,
    cron_schedule="0 * * * *",  # Run every hour
)
