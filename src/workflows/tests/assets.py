import logging
import sys

from dagster import asset, AssetExecutionContext

@asset(key_prefix=["tests"], 
    group_name="tests_logging",
    description="Printed logs", 
    kinds=["python"],
    )
def printed_logs(context: AssetExecutionContext):
    # Default dagster logger    
    context.log.debug("dagster logger debug")
    context.log.info("dagster logger info")
    context.log.warning("dagster logger warning")
    context.log.error("dagster logger error")
    context.log.exception("dagster logger exception")
    context.log.critical("dagster logger critical")

    # Python logger
    logger = logging.getLogger("unmanaged_logger")
    logger.setLevel(logging.DEBUG)
    logger.debug("python logger debug")
    logger.info("python logger info")
    logger.warning("python logger warning")
    logger.error("python logger error")
    logger.exception("python logger exception")
    logger.critical("python logger critical")

    # Python stdout logger 
    stdout_logger = logging.getLogger("stdout_logger")
    stdout_logger.setLevel(logging.DEBUG)
    #stdout_logger.addHandler(logging.StreamHandler(sys.stdout))
    stdout_logger.propagate = False
    stdout_logger.debug("stdout logger debug")
    stdout_logger.info("stdout logger info")
    stdout_logger.warning("stdout logger warning")
    stdout_logger.error("stdout logger error")
    stdout_logger.exception("stdout logger exception")
    stdout_logger.critical("stdout logger critical")

    # python stderr logger
    stderr_logger = logging.getLogger("stderr_logger")
    stderr_logger.setLevel(logging.DEBUG)
    #stderr_logger.addHandler(logging.StreamHandler(sys.stderr))
    stderr_logger.propagate = False
    stderr_logger.debug("stderr logger debug")
    stderr_logger.info("stderr logger info")
    stderr_logger.warning("stderr logger warning")
    stderr_logger.error("stderr logger error")
    stderr_logger.exception("stderr logger exception")
    stderr_logger.critical("stderr logger critical")

    return None

@asset(key_prefix=["tests"], 
    group_name="tests_fail",
    description="Failed asset", 
    kinds=["python"],
    )
def failed_asset(context: AssetExecutionContext):
    context.log.error("failed asset error")
    raise Exception("failed asset exception")