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
def create_pg_db(postgresql_details: dict = None, db_name: str = 'blob_matching'):
    """
    Creates a PostgreSQL database if it doesn't already exist.

    Args:
        postgresql_details (dict): Connection details for the PostgreSQL instance.
        db_name (str): Name of the database to create.
    """
    if postgresql_details is None:
        postgresql_details = pg_details()

    conn = psycopg2.connect(**postgresql_details)
    cur = conn.cursor()
    
    conn.commit()  # Ensure no open transaction block
    conn.autocommit = True  # Required for creating a database

    # Create database if it doesn't exist
    cur.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [db_name])
    if not cur.fetchone():
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
        print(f"Database {db_name} created.")
    else:
        print(f"Database {db_name} already exists.")

    cur.close()
    conn.close()

@timing_decorator
def create_pg_table(postgresql_details: dict = None, db_name: str = 'blob_matching', table_name: str = '', data: list = None, truncate: bool = False):
    """
    Creates a table in PostgreSQL and inserts data. Optionally truncates the table first.

    Args:
        postgresql_details (dict): Connection details for PostgreSQL.
        db_name (str): Name of the target database.
        table_name (str): Name of the table to create.
        data (list): List of tuples to insert into the table.
        truncate (bool): If True, deletes existing records before inserting new ones.
    """
    if postgresql_details is None:
        postgresql_details = pg_details()
    
    if not table_name:
        raise ValueError("Table name must be specified.")
    
    if data is None:
        data = []

    # Connect to the database
    postgresql_details['dbname'] = db_name
    conn = psycopg2.connect(**postgresql_details)
    cur = conn.cursor()

    # Ensure pgcrypto extension exists & create table
    cur.execute(sql.SQL(f"""
        CREATE EXTENSION IF NOT EXISTS "pgcrypto";
        CREATE TABLE IF NOT EXISTS {table_name} (
            geometry TEXT,
            id UUID PRIMARY KEY DEFAULT gen_random_uuid()
        );
    """))
    conn.commit()

    # **New: Optionally truncate the table**
    if truncate:
        cur.execute(sql.SQL(f"TRUNCATE TABLE {table_name};"))
        conn.commit()
        print(f"Table {table_name} truncated.")

    # Insert data if available
    if data:
        cur.executemany(sql.SQL(f'INSERT INTO {table_name} (geometry, id) VALUES (%s, %s);'), data)
        conn.commit()
        print(f"Inserted {len(data)} records into {table_name}.")
    else:
        print(f"No data provided for {table_name}.")

    cur.close()
    conn.close()

@timing_decorator
def retrieve_pg_table(postgresql_details: dict = None, db_name: str = 'blob_matching', table_name: str = ''):
    """
    Retrieves data from a PostgreSQL table and converts WKT back to geometries.

    Args:
        postgresql_details (dict): Connection details for PostgreSQL.
        db_name (str): Name of the database.
        table_name (str): Name of the table to retrieve data from.

    Returns:
        pd.DataFrame: DataFrame with geometry converted back from WKT.
    """
    if postgresql_details is None:
        postgresql_details = pg_details()
    
    if not table_name:
        raise ValueError("Table name must be specified.")

    # Connect to the database
    postgresql_details['dbname'] = db_name
    conn = psycopg2.connect(**postgresql_details)
    cur = conn.cursor()

    # Retrieve data
    cur.execute(sql.SQL(f"SELECT geometry, id FROM {table_name};"))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    # Convert to DataFrame & reapply geometry
    df = pd.DataFrame(rows, columns=["geometry", "id"])
    df["geometry"] = df["geometry"].apply(loads)  # Convert WKT to Shapely geometry
    print(f"Retrieved {len(df)} records from {table_name}.")
    
    return df