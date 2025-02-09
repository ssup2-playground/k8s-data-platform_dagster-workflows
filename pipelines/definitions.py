import os

from dagster import Definitions, multiprocess_executor, fs_io_manager
from dagster_aws.s3 import s3_pickle_io_manager, s3_resource
from dagster_k8s import k8s_job_executor

from job_serial import schedule_serial_op, serial
from job_parellel import schedule_parallel_op, parallel
from job_asset_serial import schedule_asset_serial, asset_serial, jas_generate_numbers, jas_filter_even_numbers, jas_sum_numbers, jas_multiply_result
from job_asset_parallel import schedule_asset_parallel, asset_parallel, jap_generate_numbers, jap_filter_even_numbers, jap_filter_odd_numbers, jap_sum_even_numbers, jap_sum_odd_numbers, jap_total_sum

EXECUTION_ENV = os.getenv('EXECUTION_ENV', 'local')

def get_executor():
    if EXECUTION_ENV == 'k8s':
        return k8s_job_executor
    else:
        return multiprocess_executor

def get_io_manager():
    if EXECUTION_ENV == 'k8s':
        return {
            "io_manager": s3_pickle_io_manager.configured({
                "s3_bucket": "dagster",
                "s3_prefix": "pipelines",
            }),
            "s3": s3_resource.configured({
                "endpoint_url": "https://minio.minio:9000",
                "aws_access_key_id": "root",
                "aws_secret_access_key": "root123!",
            })
        }
    else:
        return {"io_manager": fs_io_manager}

defs = Definitions(
    assets=[jas_generate_numbers, jas_filter_even_numbers, jas_sum_numbers, jas_multiply_result, jap_generate_numbers, jap_filter_even_numbers, jap_filter_odd_numbers, jap_sum_even_numbers, jap_sum_odd_numbers, jap_total_sum],
    schedules=[schedule_serial_op, schedule_parallel_op, schedule_asset_serial, schedule_asset_parallel],
    jobs=[serial, parallel, asset_serial, asset_parallel],
    executor=get_executor(),
    resources=get_io_manager()
)
