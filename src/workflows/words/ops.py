from dagster import op, OpExecutionContext, Nothing
from dagster_k8s import k8s_job_op, execute_k8s_job

echo_hello_external_job_op = k8s_job_op.configured(
    {
        "image": "busybox",
        "command": ["/bin/sh", "-c"],
        "args": ["echo HELLO"],
        "resources": {
            "requests": {"cpu": "125m", "memory": "256Mi"},
            "limits": {"cpu": "250m", "memory": "512Mi"},
        }
    },
    name="echo_hello_external_job_op",
    description="Echo hello",
)

@op(description="Echo goodbye")
def echo_goodbye_external_job_k8s(context: OpExecutionContext, Nothing):
    execute_k8s_job(
        context=context,
        image="busybox",
        command=["/bin/sh", "-c"],
        args=["echo GOODBYE"],
        resources={
            "requests": {"cpu": "125m", "memory": "256Mi"},
            "limits": {"cpu": "250m", "memory": "512Mi"},
        }
    )