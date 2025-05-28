from dagster import build_schedule_from_partitioned_job

from workflows.weather.jobs import process_weather_southkorea_hourly, process_weather_southkorea_daily

schedule_weather_southkorea_hourly = build_schedule_from_partitioned_job(
    job=process_weather_southkorea_hourly,
)

schedule_weather_southkorea_daily = build_schedule_from_partitioned_job(
    job=process_weather_southkorea_daily,
)
