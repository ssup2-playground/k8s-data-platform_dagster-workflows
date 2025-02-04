from dagster import asset, MetadataValue, ScheduleDefinition, AssetSelection, define_asset_job

@asset
def as_generate_numbers():
    """Generate a list of numbers"""
    numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    return MetadataValue.json(numbers), numbers

@asset
def as_filter_even_numbers(as_generate_numbers):
    """Filter even numbers from the generated list"""
    even_numbers = [num for num in as_generate_numbers if num % 2 == 0]
    return MetadataValue.json(even_numbers), even_numbers

@asset
def as_sum_numbers(as_filter_even_numbers):
    """Calculate the sum of the filtered even numbers"""
    sum_result = sum(as_filter_even_numbers)
    return MetadataValue.int(sum_result), sum_result

@asset
def as_multiply_result(as_sum_numbers):
    """Multiply the sum by 2"""
    result = as_sum_numbers * 2
    return MetadataValue.int(result), result


serial_asset = define_asset_job(
    "asset_serial",
    selection=AssetSelection.all()
)

schedule_serial_asset = ScheduleDefinition(
    job=serial_asset,
    cron_schedule="* * * * *",  # Runs every minute
)
