from dagster import HourlyPartitionsDefinition, DailyPartitionsDefinition

hourly_southkorea_weather_partitions = HourlyPartitionsDefinition(
    start_date="2025-06-01-00:00",
    end_offset=-24,
    timezone="Asia/Seoul",
)

daily_southkorea_weather_partitions = DailyPartitionsDefinition(
    start_date="2025-06-01",
    end_offset=-1,
    timezone="Asia/Seoul",
)
