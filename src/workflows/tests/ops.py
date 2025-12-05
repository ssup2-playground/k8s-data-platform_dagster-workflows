from dagster import op, OpExecutionContext

@op(description="Printing logs")
def printing_logs(context: OpExecutionContext):
    context.log.debug("op debug")
    context.log.info("op info")
    context.log.warning("op warning")
    context.log.error("op error")
    context.log.exception("op exception")
    context.log.critical("op critical")
    return None