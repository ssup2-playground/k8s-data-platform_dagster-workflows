from dagster import job, define_asset_job, AssetSelection
from dagster_k8s import k8s_job_executor
from dagster_celery_k8s import celery_k8s_job_executor

from workflows.numbers.ops import generate_numbers, filter_even_numbers, filter_odd_numbers, sum_even_numbers, sum_odd_numbers, sum_two_sums

# Process numbers with multi processes
@job(tags={
        "domain": "numbers",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "2000m", "memory": "4096Mi"},
                    "limits": {"cpu": "2000m", "memory": "4096Mi"},
                }
            }
        }
    })
def process_numbers():
    numbers = generate_numbers()
    
    even_numbers = filter_even_numbers(numbers)
    odd_numbers = filter_odd_numbers(numbers)

    even_sum = sum_even_numbers(even_numbers)
    odd_sum = sum_odd_numbers(odd_numbers)

    sum_two_sums(even_sum, odd_sum)

# Process numbers with multiple k8s pods
@job(executor_def=k8s_job_executor,
    tags={
        "domain": "numbers",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "2000m", "memory": "4096Mi"},
                    "limits": {"cpu": "2000m", "memory": "4096Mi"},
                }
            }
        }
    })
def process_numbers_k8s():
    numbers = generate_numbers()
    
    even_numbers = filter_even_numbers(numbers)
    odd_numbers = filter_odd_numbers(numbers)

    even_sum = sum_even_numbers(even_numbers)
    odd_sum = sum_odd_numbers(odd_numbers)

    sum_two_sums(even_sum, odd_sum)

# Process numbers with multiple k8s pods and celery
@job(executor_def=celery_k8s_job_executor,
    tags={
        "domain": "numbers",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "2000m", "memory": "4096Mi"},
                    "limits": {"cpu": "2000m", "memory": "4096Mi"},
                }
            }
        }
    })
def process_numbers_k8s_celery():
    numbers = generate_numbers()
    
    even_numbers = filter_even_numbers(numbers)
    odd_numbers = filter_odd_numbers(numbers)

    even_sum = sum_even_numbers(even_numbers)
    odd_sum = sum_odd_numbers(odd_numbers)

    sum_two_sums(even_sum, odd_sum)

# Process numbers with assets
process_numbers_asset = define_asset_job(
    name="process_numbers_asset",
    selection=AssetSelection.groups("numbers"),
    tags={
        "domain": "numbers",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "2000m", "memory": "4096Mi"},
                    "limits": {"cpu": "2000m", "memory": "4096Mi"},
                }
            }
        }
    })

# Process numbers with assets and multiple k8s pods
process_numbers_asset_k8s = define_asset_job(
    name="process_numbers_asset_k8s",
    selection=AssetSelection.groups("numbers"),
    executor_def=k8s_job_executor,
    tags={
        "domain": "numbers",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "2000m", "memory": "4096Mi"},
                    "limits": {"cpu": "2000m", "memory": "4096Mi"},
                }
            }
        }
    })

# Process numbers with assets and multiple k8s pods and celery
process_numbers_asset_k8s_celery = define_asset_job(
    name="process_numbers_asset_k8s_celery",
    selection=AssetSelection.groups("numbers"),
    executor_def=celery_k8s_job_executor,
    tags={
        "domain": "numbers",
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": "2000m", "memory": "4096Mi"},
                    "limits": {"cpu": "2000m", "memory": "4096Mi"},
                }
            }
        }
    })