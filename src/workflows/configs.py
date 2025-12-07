import os
import sys
import logging

from minio import Minio

from pyiceberg.catalog.hive import HiveCatalog

import dagster as dg
from dagster import fs_io_manager
from dagster_aws.s3 import s3_pickle_io_manager, s3_resource

#sys.stderr = sys.stdout
#logging.getLogger().handlers = []

# Set configs from envs
K8S_SERVICE_ACCOUNT_NAME = os.getenv("K8S_SERVICE_ACCOUNT_NAME", "default")
K8S_POD_NAMESPACE = os.getenv("K8S_POD_NAMESPACE", "default")
K8S_POD_NAME = os.getenv("K8S_POD_NAME", "dagster-pod")
K8S_POD_UID = os.getenv("K8S_POD_UID", "uid")

HIVE_CATALOG_URI = os.getenv("HIVE_CATALOG_URI", "thrift://hive-metastore.hive-metastore:9083")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio.minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "root")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "root123!")

IO_MANAGER_TYPE = os.getenv("IO_MANAGER_TYPE", "s3")
IO_MANAGER_S3_BUCKET = os.getenv("IO_MANAGER_S3_BUCKET", "dagster")
IO_MANAGER_S3_PREFIX = os.getenv("IO_MANAGER_S3_PREFIX", "io-manager")

WEATHER_SOUTHKOREA_API_KEY = os.getenv("WEATHER_SOUTHKOREA_API_KEY", "")

# Kubernetes
def get_k8s_service_account_name() -> str:
    return K8S_SERVICE_ACCOUNT_NAME

def get_k8s_pod_namespace() -> str:
    return K8S_POD_NAMESPACE

def get_k8s_pod_name() -> str:
    return K8S_POD_NAME

def get_k8s_pod_uid() -> str:
    return K8S_POD_UID

# MinIO 
def init_minio_client() -> Minio:
    return Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)

# Iceberg
def get_iceberg_catalog() -> HiveCatalog:
    return HiveCatalog(
        "default",
        **{
            "uri": HIVE_CATALOG_URI,
            "s3.endpoint": f"http://{MINIO_ENDPOINT}",
            "s3.access-key-id": MINIO_ACCESS_KEY,
            "s3.secret-access-key": MINIO_SECRET_KEY,
            "hive.hive2-compatible": True
        }
    )

# Dagster 
def init_io_manager() -> dict:
    if IO_MANAGER_TYPE == "s3":
        return {
            "io_manager": s3_pickle_io_manager.configured({
                "s3_bucket": IO_MANAGER_S3_BUCKET,
                "s3_prefix": IO_MANAGER_S3_PREFIX,
            }),
            "s3": s3_resource.configured({
                "endpoint_url": f"http://{MINIO_ENDPOINT}",
                "use_ssl": False,
                "aws_access_key_id": MINIO_ACCESS_KEY,
                "aws_secret_access_key": MINIO_SECRET_KEY,
            })
        }
    else:
        return {"io_manager": fs_io_manager}

@dg.logger
def init_stdout_logger(init_context):
    logger_ = logging.getLogger("dagster_stdout")
    
    #dagster_core_logger = logging.getLogger("dagster")
    #dagster_core_logger.handlers = [] # 핵심 로거 핸들러 제거
    
    logger_.setLevel(logging.DEBUG)
    logger_.handlers = []
    logger_.propagate = False
    
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    logger_.addHandler(handler)
        
    return logger_

# Weather
def get_southkorea_weather_api_key() -> str:
    return WEATHER_SOUTHKOREA_API_KEY