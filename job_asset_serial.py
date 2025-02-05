from dagster import asset, MetadataValue, ScheduleDefinition, AssetSelection, define_asset_job

@asset
def jas_generate_numbers():
    """Generate a list of numbers"""
    numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    return numbers

@asset
def jas_filter_even_numbers(jas_generate_numbers):
    """Filter even numbers from the generated list"""
    even_numbers = [num for num in jas_generate_numbers if num % 2 == 0]
    return even_numbers

@asset
def jas_sum_numbers(jas_filter_even_numbers):
    """Calculate the sum of the filtered even numbers"""
    sum_result = sum(jas_filter_even_numbers)
    return sum_result

@asset
def jas_multiply_result(jas_sum_numbers):
    """Multiply the sum by 2"""
    result = jas_sum_numbers * 2
    return result


asset_serial = define_asset_job(
    name="asset_serial",
    selection=[jas_generate_numbers, jas_filter_even_numbers, jas_sum_numbers, jas_multiply_result]
)

schedule_asset_serial = ScheduleDefinition(
    job=asset_serial,
    cron_schedule="* * * * *",  # Runs every minute
)
