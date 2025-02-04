from dagster import op, job, ScheduleDefinition

@op
def js_generate_numbers():
    """Generate a list of numbers"""
    return [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

@op
def js_filter_even_numbers(numbers):
    """Filter even numbers from the input list"""
    return [num for num in numbers if num % 2 == 0]

@op
def js_sum_numbers(numbers):
    """Calculate the sum of the numbers"""
    return sum(numbers)

@op
def js_multiply_result(sum_value: int, multiplier: int = 2):
    """Multiply the final sum by a given multiplier"""
    return sum_value * multiplier

@job
def serial():
    """A job that executes in serial"""
    even_numbers = js_filter_even_numbers(js_generate_numbers())
    sum_value = js_sum_numbers(even_numbers)
    js_multiply_result(sum_value)


schedule_serial_op = ScheduleDefinition(
    job=serial,
    cron_schedule="* * * * *",  # Run every minute
)
