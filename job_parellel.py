from dagster import op, job, ScheduleDefinition

@op
def jp_generate_numbers():
    """Generate a list of numbers from 1 to 10"""
    return list(range(1, 11))

@op
def jp_filter_even_numbers(numbers):
    """Filter even numbers from the list"""
    return [num for num in numbers if num % 2 == 0]

@op
def jp_filter_odd_numbers(numbers):
    """Filter odd numbers from the list"""
    return [num for num in numbers if num % 2 != 0]

@op
def jp_sum_numbers(numbers):
    """Calculate the sum of the given list of numbers"""
    return sum(numbers)

@op
def jp_sum_two_numbers(first_number, second_number):
    """Sum two numbers"""
    return first_number + second_number

@job
def parallel():
    """A job that executes in parallel"""
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
