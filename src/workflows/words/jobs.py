from dagster import define_asset_job, AssetSelection
from dagster_k8s import k8s_job_executor

# Process words with assets
process_words_asset = define_asset_job(
    name="process_words_asset",
    selection=AssetSelection.groups("words"),
    tags={
        "domain": "words",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "2000m", "memory": "4096Mi"},
                    "limits": {"cpu": "2000m", "memory": "4096Mi"},
                }
            }
        }
    })

# Process words with assets and multiple k8s pods
process_words_asset_k8s = define_asset_job(
    name="process_words_asset_k8s",
    selection=AssetSelection.groups("words"),
    executor_def=k8s_job_executor,
    tags={
        "domain": "words",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "2000m", "memory": "4096Mi"},
                    "limits": {"cpu": "2000m", "memory": "4096Mi"},
                }
            }
        }
    })