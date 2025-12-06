import logging

from dagster import op, OpExecutionContext

@op(description="Printing logs")
def printing_logs(context: OpExecutionContext):
    # Default dagster logger
    context.log.debug("dagster op debug")
    context.log.info("dagster op info")
    context.log.warning("dagster op warning")
    context.log.error("dagster op error")
    context.log.exception("dagster op exception")
    context.log.critical("dagster op critical")

    # Python logger without dagster managed
    unmanaged_logger = logging.getLogger("unmanaged_logger")
    unmanaged_logger.debug("unmanaged op debug")
    unmanaged_logger.info("unmanaged op info")
    unmanaged_logger.warning("unmanaged op warning")
    unmanaged_logger.error("unmanaged op error")
    unmanaged_logger.exception("unmanaged op exception")
    unmanaged_logger.critical("unmanaged op critical")

    # Python logger with dagster managed
    managed_logger = logging.getLogger("managed_logger")
    managed_logger.debug("managed op debug")
    managed_logger.info("managed op info")
    managed_logger.warning("managed op warning")
    managed_logger.error("managed op error")
    managed_logger.exception("managed op exception")
    managed_logger.critical("managed op critical")
    return None

@op(description="Failing op")
def failing_op(context: OpExecutionContext):
    context.log.error("failing op error")
    raise Exception("failing op exception")
