from dagster import asset, ScheduleDefinition, define_asset_job

@asset(description="Generate a list of numbers", kinds=["python"], group_name="ssup2", tags={"parallel": "false"},
    op_tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "200m", "memory": "32Mi"},
                }
            },
        }
    })
def jas_generate_numbers():
    numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    return numbers

@asset(description="Filter even numbers from the generated list", kinds=["python"], group_name="ssup2", tags={"parallel": "false"},
    op_tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "200m", "memory": "32Mi"},
                }
            },
        }
    })
def jas_filter_even_numbers(jas_generate_numbers):
    even_numbers = [num for num in jas_generate_numbers if num % 2 == 0]
    return even_numbers

@asset(description="Calculate the sum of the filtered even numbers", kinds=["python"], group_name="ssup2", tags={"parallel": "false"},
    op_tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "200m", "memory": "32Mi"},
                }
            },
        }
    })
def jas_sum_numbers(jas_filter_even_numbers):
    sum_result = sum(jas_filter_even_numbers)
    return sum_result

@asset(description="Multiply the sum by 2", kinds=["python"], group_name="ssup2", tags={"parallel": "false"},
    op_tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "200m", "memory": "32Mi"},
                }
            },
        }
    })
def jas_multiply_result(jas_sum_numbers):
    result = jas_sum_numbers * 2
    return result


asset_serial = define_asset_job(
    name="asset_serial",
    description="A job that executes in serial with assets",
    selection=[jas_generate_numbers, jas_filter_even_numbers, jas_sum_numbers, jas_multiply_result],
    tags={"parallel": "false", "asset": "true"}
)

schedule_asset_serial = ScheduleDefinition(
    job=asset_serial,
    cron_schedule="* * * * *",  # Runs every minute
)
