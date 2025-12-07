from dagster import job, define_asset_job, multiprocess_executor, AssetSelection

from workflows.tests.ops import printing_logs, failing_op

@job()
def print_logs():
    import logging
    import sys
    """모든 로거와 핸들러 출력"""
    for name, logger in logging.Logger.manager.loggerDict.items():
        if isinstance(logger, logging.Logger):
            print(f"Logger: {name}")
            print(f"  handlers: {logger.handlers}")
            print(f"  propagate: {logger.propagate}")
            for h in logger.handlers:
                if isinstance(h, logging.StreamHandler):
                    stream_name = "stdout" if h.stream == sys.stdout else "stderr" if h.stream == sys.stderr else "other"
                    print(f"    -> StreamHandler: {stream_name}")
    
    # root logger도 확인
    root = logging.getLogger()
    print(f"Root logger handlers: {root.handlers}")
    for h in root.handlers:
        if isinstance(h, logging.StreamHandler):
            stream_name = "stdout" if h.stream == sys.stdout else "stderr" if h.stream == sys.stderr else "other"
            print(f"  -> StreamHandler: {stream_name}")

    printing_logs()


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