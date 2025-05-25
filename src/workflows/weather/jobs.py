from dagster import define_asset_job, AssetSelection

# Process numbers with assets
process_weather_southkorea = define_asset_job(
    name="process_weather_southkorea",
    selection=AssetSelection.groups("southkorea"),
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
