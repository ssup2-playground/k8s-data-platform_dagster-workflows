from dagster import HourlyPartitionsDefinition

hourly_southkorea_weather_partitions = HourlyPartitionsDefinition(
    start_date="2023-01-01-00:00",
    end_offset=-24,
    timezone="Asia/Seoul",
)