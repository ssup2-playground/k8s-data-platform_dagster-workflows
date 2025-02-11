from dagster import op, job, ScheduleDefinition

@op(description="Generate a list of numbers from 1 to 10",
    tags={
        "parallel": "true",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "200m", "memory": "32Mi"},
                }
            },
        }
    })
def jp_generate_numbers():
    return list(range(1, 11))

@op(description="Filter even numbers from the list",
    tags={
        "parallel": "true",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "200m", "memory": "32Mi"},
                }
            },
        }
    })
def jp_filter_even_numbers(numbers):
    return [num for num in numbers if num % 2 == 0]

@op(description="Filter odd numbers from the list",
    tags={
        "parallel": "true",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "200m", "memory": "32Mi"},
                }
            },
        }
    })
def jp_filter_odd_numbers(numbers):
    return [num for num in numbers if num % 2 != 0]

@op(description="Calculate the sum of the given list of numbers",
    tags={
        "parallel": "true",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "200m", "memory": "32Mi"},
                }
            },
        }
    })
def jp_sum_numbers(numbers):
    return sum(numbers)

@op(description="Sum two numbers",
    tags={
        "parallel": "true",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "200m", "memory": "32Mi"},
                }
            },
        }
    })
def jp_sum_two_numbers(first_number, second_number):
    return first_number + second_number

@job(description="A job that executes in parallel", 
    tags={
        "parallel": "true",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "200m", "memory": "32Mi"},
                }
            },
        }
    })
def parallel():
    numbers = jp_generate_numbers()
    
    # filter_even_numbers and filter_odd_numbers run in parallel
    even_numbers = jp_filter_even_numbers(numbers)
    odd_numbers = jp_filter_odd_numbers(numbers)

    # After filtering, sum_numbers runs separately for each list
    even_sum = jp_sum_numbers(even_numbers)
    odd_sum = jp_sum_numbers(odd_numbers)

    # Sum the two sums
    jp_sum_two_numbers(even_sum, odd_sum)


schedule_parallel_op = ScheduleDefinition(
    job=parallel,
    cron_schedule="* * * * *",  # Run every minute
)
