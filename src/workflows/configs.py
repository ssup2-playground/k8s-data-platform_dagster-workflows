import os

from dagster_aws.s3 import s3_pickle_io_manager, s3_resource
from dagster_pyspark import PySparkResource

from minio import Minio
from pyiceberg.catalog.hive import HiveCatalog

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
def get_s3_resource() -> s3_resource:
    return s3_resource.configured({
        "endpoint_url": f"http://{MINIO_ENDPOINT}",
        "use_ssl": False,
        "aws_access_key_id": MINIO_ACCESS_KEY,
        "aws_secret_access_key": MINIO_SECRET_KEY,
    })

def get_io_manager_resource() -> s3_pickle_io_manager:
    return s3_pickle_io_manager.configured({
        "s3_bucket": IO_MANAGER_S3_BUCKET,
        "s3_prefix": IO_MANAGER_S3_PREFIX,
    })

def get_pyspark_resource() -> PySparkResource:
    """Get PySparkResource configured for Kubernetes"""
    return PySparkResource(
        spark_config={
            "spark.master": "k8s://https://kubernetes.default.svc:443",
            "spark.kubernetes.container.image": "ghcr.io/ssup2-playground/k8s-data-platform_spark-jobs:0.1.10",
            "spark.kubernetes.namespace": K8S_POD_NAMESPACE,
            "spark.executor.instances": "2",
            "spark.executor.memory": "2g",
            "spark.executor.cores": "1",
            # Kubernetes API server authentication
            "spark.kubernetes.authenticate.driver.serviceAccountName": K8S_SERVICE_ACCOUNT_NAME,
            "spark.kubernetes.authenticate.serviceAccountName": K8S_SERVICE_ACCOUNT_NAME,
            "spark.kubernetes.authenticate.caCertFile": "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
            "spark.kubernetes.authenticate.oauthTokenFile": "/var/run/secrets/kubernetes.io/serviceaccount/token",
            # Spark packages
            "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
            "spark.jars.ivy": "/tmp/.ivy",
            # Event log
            "spark.eventLog.enabled": "true",
            "spark.eventLog.dir": "s3a://spark/logs",
            # Monitoring
            "spark.ui.prometheus.enabled": "true",
            # Common MinIO/S3 settings
            "spark.hadoop.fs.s3a.endpoint": f"http://{MINIO_ENDPOINT}",
            "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
            "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        }
    )

# Weather
def get_southkorea_weather_api_key() -> str:
    return WEATHER_SOUTHKOREA_API_KEY