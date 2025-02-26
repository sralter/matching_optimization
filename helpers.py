import functools
import time
import logging
import csv
import os
import psutil
from datetime import datetime

class Timer:
    """A class-based decorator for timing and profiling function execution."""
    
    RESULTS_FILE = "timing_results.csv"

    def __init__(self, log_to_console=True, log_to_file=True, track_resources=True):
        """
        Initialize the Timer class.

        :param log_to_console: Whether to print logs to the console.
        :param log_to_file: Whether to save logs to a file.
        :param track_resources: Whether to track CPU and memory usage.
        """
        self.log_to_console = log_to_console
        self.log_to_file = log_to_file
        self.track_resources = track_resources
        
        # Set up logging
        logging.basicConfig(
            filename="timing.log",
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )

    def __call__(self, func):
        """Make the class instance callable as a decorator."""
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            cpu_start = psutil.cpu_percent(interval=None) if self.track_resources else None
            mem_start = psutil.virtual_memory().used / (1024 ** 2) if self.track_resources else None

            result = func(*args, **kwargs)

            elapsed_time = time.time() - start_time
            cpu_end = psutil.cpu_percent(interval=None) if self.track_resources else None
            mem_end = psutil.virtual_memory().used / (1024 ** 2) if self.track_resources else None

            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_message = f"Function `{func.__name__}` executed in {elapsed_time:.4f} sec"

            if self.track_resources:
                cpu_usage = cpu_end - cpu_start if cpu_start is not None else None
                mem_usage = mem_end - mem_start if mem_start is not None else None
                log_message += f", CPU: {cpu_usage:.2f}%, Memory: {mem_usage:.2f}MB"

            if self.log_to_console:
                print(log_message)
            logging.info(log_message)

            if self.log_to_file:
                self._save_to_csv(timestamp, func.__name__, elapsed_time, cpu_usage, mem_usage)

            return result

        return wrapper

    def _save_to_csv(self, timestamp, function_name, elapsed_time, cpu_usage, mem_usage):
        """Save timing and resource results to a CSV file."""
        file_exists = os.path.isfile(self.RESULTS_FILE)

        with open(self.RESULTS_FILE, mode="a", newline="") as file:
            writer = csv.writer(file)
            if not file_exists:
                writer.writerow(["Timestamp", "Function Name", "Execution Time (s)", "CPU Usage (%)", "Memory Usage (MB)"])
            writer.writerow([timestamp, function_name, elapsed_time, cpu_usage, mem_usage])

# Create an instance for easy use in `helpers.py`
timing_decorator = Timer(log_to_console=True, log_to_file=True, track_resources=True)
