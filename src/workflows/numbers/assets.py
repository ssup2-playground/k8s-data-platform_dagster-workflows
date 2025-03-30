from dagster import asset

@asset(key_prefix=["examples"], 
    group_name="numbers",
    description="Generate a list of numbers from 1 to 10", 
    kinds=["python"],
    op_tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "2000m", "memory": "4096Mi"},
                }
            },
        }
    })
def generated_numbers():
    return list(range(1, 11))

@asset(key_prefix=["examples"],
    group_name="numbers",
    description="Filter even numbers from the list", 
    kinds=["python"],
    op_tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "2000m", "memory": "4096Mi"},
                }
            },
        }
    })
def filtered_even_numbers(generated_numbers):
    return [num for num in generated_numbers if num % 2 == 0]

@asset(key_prefix=["examples"],
    group_name="numbers",
    description="Filter odd numbers from the list", 
    kinds=["python"],
    op_tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "2000m", "memory": "4096Mi"},
                }
            },
        }
    })
def filtered_odd_numbers(generated_numbers):
    return [num for num in generated_numbers if num % 2 != 0]

@asset(key_prefix=["examples"],
    group_name="numbers",
    description="Calculate the sum of the even numbers", 
    kinds=["python"],
    op_tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "2000m", "memory": "4096Mi"},
                }
            },
        }
    })
def summed_even_numbers(filtered_even_numbers):
    return sum(filtered_even_numbers)

@asset(key_prefix=["examples"],
    group_name="numbers",
    description="Calculate the sum of the odd numbers",
    kinds=["python"],
    op_tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "2000m", "memory": "4096Mi"},
                }
            },
        }
    })
def summed_odd_numbers(filtered_odd_numbers):
    return sum(filtered_odd_numbers)

@asset(key_prefix=["examples"],
    group_name="numbers",
    description="Sum the two sums",
    kinds=["python"],
    op_tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "2000m", "memory": "4096Mi"},
                }
            },
        }
    })
def summed_two_numbers(summed_even_numbers, summed_odd_numbers):
    return summed_even_numbers + summed_odd_numbers