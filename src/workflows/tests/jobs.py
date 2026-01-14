from dagster import job, define_asset_job, multiprocess_executor, AssetSelection

from workflows.tests.ops import printing_logs, printing_hello_k8s_job_op, printing_hello_execute_k8s_job, failing_op

@job()
def print_logs():
    printing_logs()

@job()
def print_hello_k8s_job_op():
    printing_hello_k8s_job_op()

@job()
def print_hello_execute_k8s_job():
    printing_hello_execute_k8s_job()

@job()
def fail_job():
    failing_op()

print_logs_asset= define_asset_job(
    name="print_logs_asset",
    selection=AssetSelection.groups("tests_logging"),
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

failed_job_asset = define_asset_job(
    name="failed_job_asset",
    selection=AssetSelection.groups("tests_fail"),
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