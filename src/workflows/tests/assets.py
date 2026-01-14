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
    return None

@asset(key_prefix=["tests"], 
    group_name="tests_fail",
    description="Failed asset", 
    kinds=["python"],
    )
def failed_asset(context: AssetExecutionContext):
    context.log.error("failed asset error")
    raise Exception("failed asset exception")