from dagster import asset, define_asset_job, ScheduleDefinition

@asset(description="Generate a list of numbers from 1 to 10", kinds=["python"], owners=["team:ssup2"], tags={"parallel": "true"})
def jap_generate_numbers():
    return list(range(1, 11))

@asset(description="Filter even numbers from the list", kinds=["python"], owners=["team:ssup2"], tags={"parallel": "true"})
def jap_filter_even_numbers(jap_generate_numbers):
    return [num for num in jap_generate_numbers if num % 2 == 0]

@asset(description="Filter odd numbers from the list", kinds=["python"], owners=["team:ssup2"], tags={"parallel": "true"})
def jap_filter_odd_numbers(jap_generate_numbers):
    return [num for num in jap_generate_numbers if num % 2 != 0]

@asset(description="Calculate the sum of the even numbers", kinds=["python"], owners=["team:ssup2"], tags={"parallel": "true"})
def jap_sum_even_numbers(jap_filter_even_numbers):
    return sum(jap_filter_even_numbers)

@asset(description="Calculate the sum of the odd numbers", kinds=["python"], owners=["team:ssup2"], tags={"parallel": "true"})
def jap_sum_odd_numbers(jap_filter_odd_numbers):
    return sum(jap_filter_odd_numbers)

@asset(description="Sum the two sums", kinds=["python"], owners=["team:ssup2"], tags={"parallel": "true"})
def jap_total_sum(jap_sum_even_numbers, jap_sum_odd_numbers):
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