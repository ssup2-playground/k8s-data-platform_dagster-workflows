from dagster import Definitions
from job_serial import schedule_serial_op, serial
from job_parellel import schedule_parallel_op, parallel
from asset_serial import schedule_serial_asset, serial_asset, as_generate_numbers, as_filter_even_numbers, as_sum_numbers, as_multiply_result

defs = Definitions(
    assets=[as_generate_numbers, as_filter_even_numbers, as_sum_numbers, as_multiply_result],
    schedules=[schedule_serial_op, schedule_parallel_op, schedule_serial_asset],
    jobs=[serial, parallel, serial_asset]
)
