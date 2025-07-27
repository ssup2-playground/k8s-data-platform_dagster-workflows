import uuid

from kubernetes import client, config, watch
from workflows.configs import get_k8s_service_account_name, get_k8s_pod_namespace, get_k8s_pod_name, get_k8s_pod_uid

def execute_spark_job(context, job_name_prefix: str, job_script: str, job_args: list, 
                     spark_image: str, jars: list, timeout_seconds: int = 600):
    """Execute a Spark job on Kubernetes"""
    # Get job name with unique suffix
    spark_job_name = f"{job_name_prefix}-{str(uuid.uuid4())[:8]}"
    if len(spark_job_name) > 63:
        spark_job_name = spark_job_name[:63]

    # Get dagster pod info
    dagster_pod_service_account_name = get_k8s_service_account_name()
    dagster_pod_namespace = get_k8s_pod_namespace()
    dagster_pod_name = get_k8s_pod_name()
    dagster_pod_uid = get_k8s_pod_uid()

    # Init kubernetes client
    config.load_incluster_config()
    k8s_client = client.CoreV1Api()

    # Create spark driver service
    spark_driver_service = client.V1Service(
        api_version="v1",
        kind="Service",
        metadata=client.V1ObjectMeta(
            name=spark_job_name,
            owner_references=[
                client.V1OwnerReference(
                    api_version="v1",
                    kind="Pod",
                    name=dagster_pod_name,
                    uid=dagster_pod_uid
                )
            ],
        ),
        spec=client.V1ServiceSpec(
            selector={"spark": spark_job_name},
            ports=[
                client.V1ServicePort(port=7077, target_port=7077)
            ],
            cluster_ip="None"
        )
    )

    try:
        k8s_client.create_namespaced_service(
            namespace=dagster_pod_namespace,
            body=spark_driver_service
        )
        context.log.info(f"Spark driver service created for {spark_job_name}")
    except Exception as e:
        context.log.error(f"Error creating spark driver service: {e}")
        raise e

    # Create spark driver pod
    spark_driver_job = client.V1Pod(
        api_version="v1",
        kind="Pod",
        metadata=client.V1ObjectMeta(
            name=spark_job_name,
            labels={
                "spark": spark_job_name
            },
            annotations={
                "prometheus.io/scrape": "true",
                "prometheus.io/path": "/metrics/executors/prometheus",
                "prometheus.io/port": "4040"
            },
            owner_references=[
                client.V1OwnerReference(
                    api_version="v1",
                    kind="Pod",
                    name=dagster_pod_name,
                    uid=dagster_pod_uid
                )
            ]
        ),
        spec=client.V1PodSpec(
            service_account_name=dagster_pod_service_account_name,
            restart_policy="Never",
            automount_service_account_token=True,
            containers=[
                client.V1Container(
                    name="spark-driver",
                    image=spark_image,
                    args=[
                        "spark-submit",
                        "--master", "k8s://kubernetes.default.svc.cluster.local.:443",
                        "--deploy-mode", "client",
                        "--name", f"{spark_job_name}",
                        "--conf", "spark.driver.host=" + f"{spark_job_name}.{dagster_pod_namespace}.svc.cluster.local.",
                        "--conf", "spark.driver.port=7077",
                        "--conf", "spark.executor.cores=1",
                        "--conf", "spark.executor.memory=1g",
                        "--conf", "spark.executor.instances=2",
                        "--conf", "spark.pyspark.python=/app/.venv/bin/python3",
                        "--conf", "spark.jars.packages=" + ",".join(jars),
                        "--conf", "spark.jars.ivy=/tmp/.ivy",
                        "--conf", "spark.kubernetes.namespace=" + f"{dagster_pod_namespace}",
                        "--conf", "spark.kubernetes.driver.pod.name=" + f"{spark_job_name}",
                        "--conf", "spark.kubernetes.executor.podNamePrefix=" + f"{spark_job_name}",
                        "--conf", "spark.kubernetes.container.image=" + f"{spark_image}",
                        "--conf", "spark.kubernetes.executor.request.cores=1",
                        "--conf", "spark.kubernetes.executor.limit.cores=2",
                        "--conf", "spark.kubernetes.authenticate.serviceAccountName=" + f"{dagster_pod_service_account_name}",
                        "--conf", "spark.kubernetes.authenticate.caCertFile=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
                        "--conf", "spark.kubernetes.authenticate.oauthTokenFile=/var/run/secrets/kubernetes.io/serviceaccount/token",
                        "--conf", "spark.eventLog.enabled=true",
                        "--conf", "spark.eventLog.dir=s3a://spark/logs",
                        "--conf", "spark.ui.prometheus.enabled=true",
                        job_script
                    ] + job_args
                )
            ]
        )
    )

    try:
        k8s_client.create_namespaced_pod(
            namespace=dagster_pod_namespace,
            body=spark_driver_job
        )
        context.log.info(f"Spark driver pod created for {spark_job_name}")
    except Exception as e:
        context.log.error(f"Error creating spark driver pod: {e}")
        raise e

    # Wait for pod to be deleted with watch
    v1 = client.CoreV1Api()
    w = watch.Watch()
    timed_out = True

    for event in w.stream(v1.list_namespaced_pod, namespace=dagster_pod_namespace, 
                         field_selector=f"metadata.name={spark_job_name}", 
                         timeout_seconds=timeout_seconds):
        pod = event["object"]
        phase = pod.status.phase
        if phase in ["Succeeded", "Failed"]:
            timed_out = False
            if phase == "Failed":
                context.log.error(f"Pod '{spark_job_name}' has terminated with status: {phase}")
                raise Exception(f"Pod '{spark_job_name}' has terminated with status: {phase}")
            else:
                context.log.info(f"Pod '{spark_job_name}' has terminated with status: {phase}")
            break

    if timed_out:
        context.log.error(f"Pod '{spark_job_name}' timed out")
        raise Exception(f"Pod '{spark_job_name}' timed out") 