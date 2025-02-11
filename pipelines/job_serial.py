from dagster import op, job, ScheduleDefinition

@op(description="Generate a list of numbers",
    tags={
        "parallel": "false",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "200m", "memory": "32Mi"},
                }
            },
        }
    })
def js_generate_numbers():
    return [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

@op(description="Filter even numbers from the input list",
    tags={
        "parallel": "false",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "200m", "memory": "32Mi"},
                }
            },
        }
    })
def js_filter_even_numbers(numbers):
    return [num for num in numbers if num % 2 == 0]

@op(description="Calculate the sum of the numbers",
    tags={
        "parallel": "false",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "200m", "memory": "32Mi"},
                }
            },
        }
    })
def js_sum_numbers(numbers):
    return sum(numbers)

@op(description="Multiply the final sum by a given multiplier",
    tags={
        "parallel": "false",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "200m", "memory": "32Mi"},
                }
            },
        }
    })
def js_multiply_result(sum_value: int, multiplier: int = 2):
    return sum_value * multiplier

@job(description="A job that executes in serial",
    tags={
        "parallel": "false",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "200m", "memory": "32Mi"},
                }
            },
        }
    })
def serial():
    even_numbers = js_filter_even_numbers(js_generate_numbers())
    sum_value = js_sum_numbers(even_numbers)
    js_multiply_result(sum_value)


schedule_serial_op = ScheduleDefinition(
    job=serial,
    cron_schedule="* * * * *",  # Run every minute
)
