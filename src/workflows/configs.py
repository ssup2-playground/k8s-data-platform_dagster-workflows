import os

from dagster import fs_io_manager
from dagster_aws.s3 import s3_pickle_io_manager, s3_resource

# Set configs
IO_MANAGER_TYPE = os.getenv("IO_MANAGER_TYPE", "s3")
IO_MANAGER_S3_ENDPOINT_URL = os.getenv("IO_MANAGER_S3_ENDPOINT_URL", "http://minio.minio:9000")
IO_MANAGER_S3_BUCKET = os.getenv("IO_MANAGER_S3_BUCKET", "dagster")
IO_MANAGER_S3_PREFIX = os.getenv("IO_MANAGER_S3_PREFIX", "io-manager")
IO_MANAGER_S3_ACCESS_KEY_ID = os.getenv("IO_MANAGER_S3_ACCESS_KEY_ID", "root")
IO_MANAGER_S3_SECRET_ACCESS_KEY = os.getenv("IO_MANAGER_S3_SECRET_ACCESS_KEY", "root123!")

# Set IO Manager S3
def get_io_manager():
    if IO_MANAGER_TYPE == "s3":
        return {
            "io_manager": s3_pickle_io_manager.configured({
                "s3_bucket": IO_MANAGER_S3_BUCKET,
                "s3_prefix": IO_MANAGER_S3_PREFIX,
            }),
            "s3": s3_resource.configured({
                "endpoint_url": IO_MANAGER_S3_ENDPOINT_URL,
                "use_ssl": False,
                "aws_access_key_id": IO_MANAGER_S3_ACCESS_KEY_ID,
                "aws_secret_access_key": IO_MANAGER_S3_SECRET_ACCESS_KEY,
            })
        }
    else:
        return {"io_manager": fs_io_manager}