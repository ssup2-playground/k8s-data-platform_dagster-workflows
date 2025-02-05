from dagster import asset, define_asset_job, ScheduleDefinition

@asset
def jap_generate_numbers():
    """Generate a list of numbers from 1 to 10"""
    return list(range(1, 11))

@asset
def jap_filter_even_numbers(jap_generate_numbers):
    """Filter even numbers from the list"""
    return [num for num in jap_generate_numbers if num % 2 == 0]

@asset
def jap_filter_odd_numbers(jap_generate_numbers):
    """Filter odd numbers from the list"""
    return [num for num in jap_generate_numbers if num % 2 != 0]

@asset
def jap_sum_even_numbers(jap_filter_even_numbers):
    """Calculate the sum of the even numbers"""
    return sum(jap_filter_even_numbers)

@asset
def jap_sum_odd_numbers(jap_filter_odd_numbers):
    """Calculate the sum of the odd numbers"""
    return sum(jap_filter_odd_numbers)

@asset
def jap_total_sum(jap_sum_even_numbers, jap_sum_odd_numbers):
    """Sum the two sums"""
    return jap_sum_even_numbers + jap_sum_odd_numbers

# Define a job that includes all assets
asset_parallel = define_asset_job(
    name="asset_parallel",
    selection=[jap_generate_numbers, jap_filter_even_numbers, jap_filter_odd_numbers, jap_sum_even_numbers, jap_sum_odd_numbers, jap_total_sum],
    tags={"parallel": "true", "asset": "true"}
)

# Schedule the job to run every minute
schedule_asset_parallel = ScheduleDefinition(
    job=asset_parallel,
    cron_schedule="* * * * *",  # Run every minute
)