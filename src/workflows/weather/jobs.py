from dagster import define_asset_job, AssetSelection

# Process weather with assets
process_weather_southkorea_hourly = define_asset_job(
    name="process_weather_southkorea_hourly",
    selection=AssetSelection.tag("schedule", "hourly"),
    tags={
        "domain": "weather",
        "no-concurrency": "weather-southkorea",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "1000m", "memory": "2048Mi"},
                    "limits": {"cpu": "2000m", "memory": "4096Mi"},
                }
            }
        }
    },
    run_tags={
        "no-concurrency": "weather-southkorea",
    }
)

process_weather_southkorea_daily = define_asset_job(
    name="process_weather_southkorea_daily",
    selection=AssetSelection.tag("schedule", "daily"),
    tags={
        "domain": "weather",
        "no-concurrency": "weather-southkorea",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "1000m", "memory": "2048Mi"},
                    "limits": {"cpu": "2000m", "memory": "4096Mi"},
                }
            }
        }
    },
    run_tags={
        "no-concurrency": "weather-southkorea",
    }
)