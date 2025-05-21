import time
from typing import Optional
from pyiceberg.table import Table
import pyarrow as pa

def retry_append_table(
        iceberg_table: Table, 
        parquet_table: pa.Table, 
        max_retries: int = 5, 
        initial_delay: float = 1.0
) -> Optional[Exception]:
    '''Retry logic for appending data to Iceberg table'''
    delay = initial_delay
    last_exception = None
    
    for attempt in range(max_retries):
        try:
            iceberg_table.append(parquet_table)
            return None
        except Exception as e:
            last_exception = e
            if attempt < max_retries - 1:
                time.sleep(delay)
                delay *= 2  # Exponential backoff
                continue
            break
    
    return last_exception