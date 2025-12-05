
from dagster import asset, AssetExecutionContext

@asset(key_prefix=["tests"], 
    group_name="tests",
    description="Printed logs", 
    kinds=["python"],
    )
def printed_logs(context: AssetExecutionContext):
    context.log.debug("asset debug")
    context.log.info("asset info")
    context.log.warning("asset warning")
    context.log.error("asset error")
    context.log.exception("asset exception")
    context.log.critical("asset critical")
    return None