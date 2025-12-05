from dagster import job, define_asset_job, multiprocess_executor, AssetSelection

from workflows.tests.ops import printing_logs

@job()
def print_logs():
    printing_logs()

print_logs_asset= define_asset_job(
    name="print_logs_asset",
    selection=AssetSelection.groups("tests"),
    executor_def=multiprocess_executor,
    tags={
        "domain": "tests",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "1000m", "memory": "2048Mi"},
                    "limits": {"cpu": "2000m", "memory": "4096Mi"},
                }
            }
        }
    }
)