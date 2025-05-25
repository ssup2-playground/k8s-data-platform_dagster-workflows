from dagster import job, define_asset_job, AssetSelection
from dagster_k8s import k8s_job_executor

from workflows.words.ops import echo_hello_external_job_op, echo_goodbye_external_job_k8s

# Process words with assets
process_words_asset = define_asset_job(
    name="process_words_asset",
    selection=AssetSelection.groups("words"),
    tags={
        "domain": "words",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "1000m", "memory": "2048Mi"},
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
                    "requests": {"cpu": "1000m", "memory": "2048Mi"},
                    "limits": {"cpu": "2000m", "memory": "4096Mi"},
                }
            }
        }
    })

# Process words with external k8s job
@job
def process_words_echo_external_k8s_job():
    echo_goodbye_external_job_k8s(echo_hello_external_job_op())