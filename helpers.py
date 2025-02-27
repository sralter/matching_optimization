import functools
import time
import logging
import csv
import os
import psutil
import json
from datetime import datetime
import pandas as pd
import geopandas as gpd
from shapely import to_wkt
from shapely.wkt import dumps, loads
from shapely.geometry import Polygon, box
from shapely.geometry.base import BaseGeometry
import random
import uuid
import psycopg2
from psycopg2 import sql

class Timer:
    """A class-based decorator for timing and profiling function execution."""

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
        self.session_id = f"sid_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"  # Unique session ID with prefix

        # Define directory and file paths
        self.log_dir = "logs"
        os.makedirs(self.log_dir, exist_ok=True)  # Ensure logs directory exists

        self.RESULTS_FILE = os.path.join(self.log_dir, f"{self.session_id}_timing_results.csv")
        self.LOG_FILE = os.path.join(self.log_dir, f"{self.session_id}_timing.log")

        # Ensure files exist and have proper headers
        self._ensure_files_exist()

        # Set up logging (reset if needed)
        logging.shutdown()  # Close previous handlers (if any)
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        logging.basicConfig(
            filename=self.LOG_FILE,
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            filemode="w",  # Start a fresh log file for each session
        )

    def _ensure_files_exist(self):
        """Ensure timing_results.csv and timing.log exist with proper headers."""
        # Ensure CSV exists and has correct headers
        if not os.path.exists(self.RESULTS_FILE):
            with open(self.RESULTS_FILE, mode="w", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(["Session ID", "Timestamp", "Function Name", "Execution Time (s)", "CPU Usage (%)", "Memory Usage (MB)", "Arguments"])
            print(f"Created fresh {self.RESULTS_FILE}")

        # Ensure log file exists
        if not os.path.exists(self.LOG_FILE):
            open(self.LOG_FILE, "w").close()  # Create empty file
            print(f"Created fresh {self.LOG_FILE}")

    def __call__(self, func):
        """Make the class instance callable as a decorator."""
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            cpu_start = psutil.cpu_percent(interval=None) if self.track_resources else None
            mem_start = psutil.virtual_memory().used / (1024 ** 2) if self.track_resources else None  # Convert to MB

            result = func(*args, **kwargs)

            elapsed_time = time.time() - start_time
            cpu_end = psutil.cpu_percent(interval=None) if self.track_resources else None
            mem_end = psutil.virtual_memory().used / (1024 ** 2) if self.track_resources else None  # Convert to MB

            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            args_repr = json.dumps({"args": args, "kwargs": kwargs}, default=str)  # Convert args to JSON

            log_message = f"Function `{func.__name__}` executed in {elapsed_time:.4f} sec"

            if self.track_resources:
                cpu_usage = abs(cpu_end - cpu_start) if cpu_start is not None else None
                mem_usage = abs(mem_end - mem_start) if mem_start is not None else None
                log_message += f", CPU: {cpu_usage:.2f}%, Memory: {mem_usage:.2f}MB"

            if self.log_to_console:
                print(log_message)
            logging.info(log_message)

            if self.log_to_file:
                self._save_to_csv(timestamp, func.__name__, elapsed_time, cpu_usage, mem_usage, args_repr)

            return result

        return wrapper

    def _save_to_csv(self, timestamp, function_name, elapsed_time, cpu_usage, mem_usage, args_repr):
        """Save timing and resource results to a CSV file with function arguments."""
        file_exists = os.path.isfile(self.RESULTS_FILE)

        with open(self.RESULTS_FILE, mode="a", newline="") as file:
            writer = csv.writer(file)
            if not file_exists:
                writer.writerow(["Session ID", "Timestamp", "Function Name", "Execution Time (s)", "CPU Usage (%)", "Memory Usage (MB)", "Arguments"])
            writer.writerow([self.session_id, timestamp, function_name, elapsed_time, cpu_usage, mem_usage, args_repr])

# Create an instance for easy use in `helpers.py`
timing_decorator = Timer(log_to_console=True, log_to_file=True, track_resources=True)

@timing_decorator
def generate_random_polygons(n=100, bbox=(-180, -90, 180, 90)):
    """Generate n random polygons inside a bounding box with a unique ID."""
    minx, miny, maxx, maxy = bbox
    polygons = []
    ids = []  # List to store UUIDs
    for _ in range(n):
        x1, y1 = random.uniform(minx, maxx), random.uniform(miny, maxy)
        x2, y2 = x1 + random.uniform(1, 2), y1 + random.uniform(1, 2)
        polygons.append(box(x1, y1, x2, y2))  # Create rectangle
        ids.append(str(uuid.uuid4()))  # Generate unique UUID
    gdf = gpd.GeoDataFrame(geometry=polygons, crs="EPSG:4326")
    gdf['id'] = ids  # Add the id column
    return gdf

@timing_decorator
def convert_col_to_string(df: gpd.GeoDataFrame, col: str = 'geometry') -> gpd.GeoDataFrame:
    """
    Converts a specified geometry column to a WKT string representation.

    Args:
        df (gpd.GeoDataFrame): The GeoDataFrame with the geometry column.
        col (str): The column that will be converted, defaults to 'geometry'.

    Returns:
        gpd.GeoDataFrame: A new GeoDataFrame with the converted column.
    """
    df = df.copy()  # Ensure we don't modify the original dataframe
    df[col] = df[col].apply(lambda geom: to_wkt(geom) if isinstance(geom, BaseGeometry) else str(geom))
    return df

@timing_decorator
def df_itertuple(df: gpd.GeoDataFrame):
    """
    Converts a GeoDataFrame into a list of tuples without index.

    Args:
        df (gpd.GeoDataFrame): The input geodataframe.

    Returns:
        list: A list of tuples representing the rows of the dataframe.
    """
    return list(df.itertuples(index=False, name=None))

def pg_details() -> dict:
    """Default PostgreSQL settings to connect to the database"""
    return {
    'dbname': 'postgres',  # Use the default database to create a new one
    'user': 'postgres',
    'password': 'rootroot',
    'host': 'localhost',
    'port': '5432'
    }

@timing_decorator
def create_pg_db(pg_details: dict = pg_details()):
    """
    Creates 
    """
    # Define connection details
    # Step 1: Connect to PostgreSQL and create a new database (if it doesn't exist)
    conn = psycopg2.connect(
        dbname=postgresql_details['dbname'],
        user=postgresql_details['user'],
        password=postgresql_details['password'],
        host=postgresql_details['host'],
        port=postgresql_details['port']
    )
    cur = conn.cursor()

    # Commit any active transactions before creating a new database
    conn.commit()  # This ensures no open transaction block

    # Set autocommit to True for creating the database (it cannot run inside a transaction block)
    conn.autocommit = True

    # Create a new database (only if it doesn't already exist)
    new_db_name = 'blob_matching'
    cur.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [new_db_name])
    if not cur.fetchone():
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(new_db_name)))
        print(f"Database {new_db_name} created.")
    else:
        print(f"Database {new_db_name} already exists.")

    # Reset autocommit to False (we want to manage transactions for the rest of the operations)
    conn.autocommit = False

    # Close connection for creating the new database
    cur.close()
    conn.close()
