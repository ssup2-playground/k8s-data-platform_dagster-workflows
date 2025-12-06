import logging

from dagster import asset, AssetExecutionContext

@asset(key_prefix=["tests"], 
    group_name="tests_logging",
    description="Printed logs", 
    kinds=["python"],
    )
def printed_logs(context: AssetExecutionContext):
    # Default dagster logger    
    context.log.debug("dagster asset debug")
    context.log.info("dagster asset info")
    context.log.warning("dagster asset warning")
    context.log.error("dagster asset error")
    context.log.exception("dagster asset exception")
    context.log.critical("dagster asset critical")

    # Python logger without dagster managed
    unmanaged_logger = logging.getLogger("unmanaged_logger")
    unmanaged_logger.debug("unmanaged asset debug")
    unmanaged_logger.info("unmanaged asset info")
    unmanaged_logger.warning("unmanaged asset warning")
    unmanaged_logger.error("unmanaged asset error")
    unmanaged_logger.exception("unmanaged asset exception")
    unmanaged_logger.critical("unmanaged asset critical")

    # Python logger with dagster managed
    managed_logger = logging.getLogger("managed_logger")
    managed_logger.debug("managed asset debug")
    managed_logger.info("managed asset info")
    managed_logger.warning("managed asset warning")
    managed_logger.error("managed asset error")
    managed_logger.exception("managed asset exception")
    managed_logger.critical("managed asset critical")
    return None

@asset(key_prefix=["tests"], 
    group_name="tests_fail",
    description="Failed asset", 
    kinds=["python"],
    )
def failed_asset(context: AssetExecutionContext):
    context.log.error("failed asset error")
    raise Exception("failed asset exception")