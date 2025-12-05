from dagster import asset, AssetExecutionContext

@asset(key_prefix=["numbers"], 
    group_name="numbers",
    description="Generated a list of numbers from 1 to 10", 
    kinds=["python"],
    op_tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "2000m", "memory": "4096Mi"},
                    "limits": {"cpu": "2000m", "memory": "4096Mi"},
                }
            },
        }
    })
def generated_numbers(context: AssetExecutionContext):
    context.log.info("Generating a list of numbers from 1 to 10")
    return list(range(1, 11))

@asset(key_prefix=["numbers"],
    group_name="numbers",
    description="Filtered even numbers from the list", 
    kinds=["python"],
    op_tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "2000m", "memory": "4096Mi"},
                    "limits": {"cpu": "2000m", "memory": "4096Mi"},
                }
            },
        }
    })
def filtered_even_numbers(context: AssetExecutionContext, generated_numbers):
    context.log.info("Filtering even numbers from the list")
    return [num for num in generated_numbers if num % 2 == 0]

@asset(key_prefix=["numbers"],
    group_name="numbers",
    description="Filtered odd numbers from the list", 
    kinds=["python"],
    op_tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "2000m", "memory": "4096Mi"},
                    "limits": {"cpu": "2000m", "memory": "4096Mi"},
                }
            },
        }
    })
def filtered_odd_numbers(context: AssetExecutionContext, generated_numbers):
    context.log.info("Filtering odd numbers from the list")
    return [num for num in generated_numbers if num % 2 != 0]

@asset(key_prefix=["numbers"],
    group_name="numbers",
    description="Summed the even numbers", 
    kinds=["python"],
    op_tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "2000m", "memory": "4096Mi"},
                    "limits": {"cpu": "2000m", "memory": "4096Mi"},
                }
            },
        }
    })
def summed_even_numbers(context: AssetExecutionContext, filtered_even_numbers):
    context.log.info("Summing the even numbers")
    return sum(filtered_even_numbers)

@asset(key_prefix=["numbers"],
    group_name="numbers",
    description="Summed the odd numbers",
    kinds=["python"],
    op_tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "2000m", "memory": "4096Mi"},
                    "limits": {"cpu": "2000m", "memory": "4096Mi"},
                }
            },
        }
    })
def summed_odd_numbers(context: AssetExecutionContext, filtered_odd_numbers):
    context.log.info("Summing the odd numbers")
    return sum(filtered_odd_numbers)

@asset(key_prefix=["numbers"],
    group_name="numbers",
    description="Summed the two sums",
    kinds=["python"],
    op_tags={
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "2000m", "memory": "4096Mi"},
                    "limits": {"cpu": "2000m", "memory": "4096Mi"},
                }
            },
        }
    })
def summed_two_sums(context: AssetExecutionContext, summed_even_numbers, summed_odd_numbers):
    context.log.info("Summing the two sums")
    return summed_even_numbers + summed_odd_numbers