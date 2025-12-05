from dagster import op, OpExecutionContext

@op(description="Generate a list of numbers from 1 to 10",
    tags={
        "domain": "numbers",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "2000m", "memory": "4096Mi"},
                    "limits": {"cpu": "2000m", "memory": "4096Mi"},
                }
            },
        }
    })
def generate_numbers(context: OpExecutionContext):
    context.log.info("Generating a list of numbers from 1 to 10")
    return list(range(1, 11))

@op(description="Filter even numbers from the list",
    tags={
        "domain": "numbers",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "2000m", "memory": "4096Mi"},
                    "limits": {"cpu": "2000m", "memory": "4096Mi"},
                }
            },
        }
    })
def filter_even_numbers(context: OpExecutionContext, numbers):
    context.log.info("Filtering even numbers from the list")
    return [num for num in numbers if num % 2 == 0]

@op(description="Filter odd numbers from the list",
    tags={
        "domain": "numbers",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "2000m", "memory": "4096Mi"},
                    "limits": {"cpu": "2000m", "memory": "4096Mi"},
                }
            },
        }
    })
def filter_odd_numbers(context: OpExecutionContext, numbers):
    context.log.info("Filtering odd numbers from the list")
    return [num for num in numbers if num % 2 != 0]

@op(description="Sum the given list of even numbers",
    tags={
        "domain": "numbers",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "2000m", "memory": "4096Mi"},
                    "limits": {"cpu": "2000m", "memory": "4096Mi"},
                }
            },
        }
    })
def sum_even_numbers(context: OpExecutionContext, numbers):
    context.log.info("Summing the even numbers")
    return sum(numbers)

@op(description="Sum the given list of odd numbers",
    tags={
        "domain": "numbers",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "2000m", "memory": "4096Mi"},
                    "limits": {"cpu": "2000m", "memory": "4096Mi"},
                }
            },
        }
    })
def sum_odd_numbers(context: OpExecutionContext, numbers):
    context.log.info("Summing the odd numbers")
    return sum(numbers)

@op(description="Sum two sums",
    tags={
        "domain": "numbers",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "2000m", "memory": "4096Mi"},
                    "limits": {"cpu": "2000m", "memory": "4096Mi"},
                }
            },
        }
    })
def sum_two_sums(context: OpExecutionContext, first_number, second_number):
    context.log.info("Summing the two sums")
    return first_number + second_number