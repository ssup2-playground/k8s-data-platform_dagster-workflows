import os

from dagster import fs_io_manager
from dagster_aws.s3 import s3_pickle_io_manager, s3_resource

# Set configs
IO_MANAGER_TYPE = os.getenv("IO_MANAGER_TYPE", "s3")

# Set IO Manager S3
def get_io_manager():
    if IO_MANAGER_TYPE == "s3":
        return {
            "io_manager": s3_pickle_io_manager.configured({
                "s3_bucket": "dagster",
                "s3_prefix": "io-manager",
            }),
            "s3": s3_resource.configured({
                "endpoint_url": "https://minio.minio:9000",
                "use_ssl": False,
                "aws_access_key_id": "root",
                "aws_secret_access_key": "root123!",
            })
        }
    else:
        return {"io_manager": fs_io_manager}