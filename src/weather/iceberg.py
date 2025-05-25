import time
from typing import Optional
from pyiceberg.table import Table
import pyarrow as pa
from pyiceberg.exceptions import CommitFailedException

def retry_append_table(
    iceberg_table: Table,
    table: pa.Table,
    max_retries: int = 5,
    initial_delay: float = 1.0
) -> Optional[Exception]:
    """Retry logic for appending data to Iceberg table with exponential backoff"""
    delay: float = initial_delay
    last_exception: Optional[Exception] = None
    
    for attempt in range(max_retries):
        try:
            iceberg_table.append(table)
            return None
        except CommitFailedException as e:
            print(f"!!!!CommitFailedException: {attempt} / {max_retries}!!!!")
            last_exception = e
            if attempt < max_retries - 1:
                time.sleep(delay)
                delay *= 2
                continue
            break
        except Exception as e:
            return e
    
    return last_exception