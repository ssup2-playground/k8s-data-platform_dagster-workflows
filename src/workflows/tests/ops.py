from dagster import op, OpExecutionContext
from dagster_k8s import k8s_job_op, execute_k8s_job

@op(description="Printing logs")
def printing_logs(context: OpExecutionContext):
    # Default dagster logger
    context.log.debug("dagster logger debug")
    context.log.info("dagster logger info")
    context.log.warning("dagster logger warning")
    context.log.error("dagster logger error")
    context.log.exception("dagster logger exception")
    context.log.critical("dagster logger critical")
    return None

@op(description="Failing op")
def failing_op(context: OpExecutionContext):
    context.log.error("failing op error")
    raise Exception("failing op exception")

printing_hello_k8s_job_op = k8s_job_op.configured(
    {
        "image": "busybox",
        "command": ["/bin/sh", "-c"],
        "args": ["echo HELLO"],
    },
    name="printing_hello_k8s_job",
    description="Printing hello with k8s_job_op",
)

@op(description="Printing hello with execute_k8s_job")
def printing_hello_execute_k8s_job(context):
    execute_k8s_job(
        context=context,
        image="busybox",
        command=["/bin/sh", "-c"],
        args=["echo HELLO"],
    )