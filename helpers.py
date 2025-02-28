import functools
import time
import logging, logging.handlers
import csv
import os
import psutil
import json
from datetime import datetime
import pandas as pd
import geopandas as gpd
from shapely import to_wkt
from shapely.wkt import dumps, loads
from shapely.geometry import Polygon, box, Point
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform
import random
import uuid
import psycopg2
from psycopg2 import sql, extras
import geohash2 as geohash
import itertools
import numpy as np
import pyproj
import multiprocessing as mp
import os

# Timer class for logging timing, CPU, and memory usage
class Timer:
    """A class-based decorator for timing and profiling function execution."""
    
    def __init__(self, log_to_console=True, log_to_file=True, track_resources=True):
        self.log_to_console = log_to_console
        self.log_to_file = log_to_file
        self.track_resources = track_resources

        # Use a fixed log file name (removing the session-specific file naming)
        self.log_dir = "logs"
        os.makedirs(self.log_dir, exist_ok=True)
        self.RESULTS_FILE = os.path.join(self.log_dir, "timing_results.csv")
        self.LOG_FILE = os.path.join(self.log_dir, "timing.log")

        # Ensure files exist and have proper headers
        self._ensure_files_exist()

        # Set up logging to append to the same log file
        logging.shutdown()
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        logging.basicConfig(
            filename=self.LOG_FILE,
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            filemode="a",  # Use append mode
        )
    
    def _ensure_files_exist(self):
        """Ensure timing_results.csv and timing.log exist with proper headers."""
        if not os.path.exists(self.RESULTS_FILE):
            with open(self.RESULTS_FILE, mode="w", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(["Timestamp", "Function Name", "Execution Time (s)", "CPU Usage (%)", "Memory Usage (MB)", "Arguments"])
            print(f"Created fresh {self.RESULTS_FILE}")
        
        if not os.path.exists(self.LOG_FILE):
            open(self.LOG_FILE, "w").close()  # Create empty file
            print(f"Created fresh {self.LOG_FILE}")

    def _save_to_csv(self, timestamp, function_name, elapsed_time, cpu_usage, mem_usage, args_repr):
        """Save timing and resource results to a CSV file with function arguments."""
        with open(self.RESULTS_FILE, mode="a", newline="") as file:
            writer = csv.writer(file)
            writer.writerow([timestamp, function_name, elapsed_time, cpu_usage, mem_usage, args_repr])
    
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
            args_repr = json.dumps({"args": args, "kwargs": kwargs}, default=str)

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

# # ✅ Shared session ID for all processes
# SESSION_ID = f"sid_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
# LOG_DIR = "logs"
# os.makedirs(LOG_DIR, exist_ok=True)

# LOG_FILE = os.path.join(LOG_DIR, f"{SESSION_ID}_timing.log")
# CSV_FILE = os.path.join(LOG_DIR, f"{SESSION_ID}_timing_results.csv")

# class Timer_original:
#     """A class-based decorator for timing and profiling function execution."""

#     def __init__(self, log_to_console=True, log_to_file=True, track_resources=True):
#         """
#         Initialize the Timer class.

#         :param log_to_console: Whether to print logs to the console.
#         :param log_to_file: Whether to save logs to a file.
#         :param track_resources: Whether to track CPU and memory usage.
#         """
#         self.log_to_console = log_to_console
#         self.log_to_file = log_to_file
#         self.track_resources = track_resources
#         self.session_id = f"sid_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"  # Unique session ID with prefix

#         # Define directory and file paths
#         self.log_dir = "logs"
#         os.makedirs(self.log_dir, exist_ok=True)  # Ensure logs directory exists

#         self.RESULTS_FILE = os.path.join(self.log_dir, f"{self.session_id}_timing_results.csv")
#         self.LOG_FILE = os.path.join(self.log_dir, f"{self.session_id}_timing.log")

#         # Ensure files exist and have proper headers
#         self._ensure_files_exist()

#         # Set up logging (reset if needed)
#         logging.shutdown()  # Close previous handlers (if any)
#         for handler in logging.root.handlers[:]:
#             logging.root.removeHandler(handler)

#         logging.basicConfig(
#             filename=self.LOG_FILE,
#             level=logging.INFO,
#             format="%(asctime)s - %(levelname)s - %(message)s",
#             filemode="w",  # Start a fresh log file for each session
#         )

#     def _ensure_files_exist(self):
#         """Ensure timing_results.csv and timing.log exist with proper headers."""
#         # Ensure CSV exists and has correct headers
#         if not os.path.exists(self.RESULTS_FILE):
#             with open(self.RESULTS_FILE, mode="w", newline="") as file:
#                 writer = csv.writer(file)
#                 writer.writerow(["Session ID", "Timestamp", "Function Name", "Execution Time (s)", "CPU Usage (%)", "Memory Usage (MB)", "Arguments"])
#             print(f"Created fresh {self.RESULTS_FILE}")

#         # Ensure log file exists
#         if not os.path.exists(self.LOG_FILE):
#             open(self.LOG_FILE, "w").close()  # Create empty file
#             print(f"Created fresh {self.LOG_FILE}")

#     def __call__(self, func):
#         """Make the class instance callable as a decorator."""
#         @functools.wraps(func)
#         def wrapper(*args, **kwargs):
#             start_time = time.time()
#             cpu_start = psutil.cpu_percent(interval=None) if self.track_resources else None
#             mem_start = psutil.virtual_memory().used / (1024 ** 2) if self.track_resources else None  # Convert to MB

#             result = func(*args, **kwargs)

#             elapsed_time = time.time() - start_time
#             cpu_end = psutil.cpu_percent(interval=None) if self.track_resources else None
#             mem_end = psutil.virtual_memory().used / (1024 ** 2) if self.track_resources else None  # Convert to MB

#             timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#             args_repr = json.dumps({"args": args, "kwargs": kwargs}, default=str)  # Convert args to JSON

#             log_message = f"Function `{func.__name__}` executed in {elapsed_time:.4f} sec"

#             if self.track_resources:
#                 cpu_usage = abs(cpu_end - cpu_start) if cpu_start is not None else None
#                 mem_usage = abs(mem_end - mem_start) if mem_start is not None else None
#                 log_message += f", CPU: {cpu_usage:.2f}%, Memory: {mem_usage:.2f}MB"

#             if self.log_to_console:
#                 print(log_message)
#             logging.info(log_message)

#             if self.log_to_file:
#                 self._save_to_csv(timestamp, func.__name__, elapsed_time, cpu_usage, mem_usage, args_repr)

#             return result

#         return wrapper

#     def _save_to_csv(self, timestamp, function_name, elapsed_time, cpu_usage, mem_usage, args_repr):
#         """Save timing and resource results to a CSV file with function arguments."""
#         file_exists = os.path.isfile(self.RESULTS_FILE)

#         with open(self.RESULTS_FILE, mode="a", newline="") as file:
#             writer = csv.writer(file)
#             if not file_exists:
#                 writer.writerow(["Session ID", "Timestamp", "Function Name", "Execution Time (s)", "CPU Usage (%)", "Memory Usage (MB)", "Arguments"])
#             writer.writerow([self.session_id, timestamp, function_name, elapsed_time, cpu_usage, mem_usage, args_repr])

# # Create an instance for easy use in `helpers.py`
# timing_decorator = Timer_original(log_to_console=True, log_to_file=True, track_resources=True)

@Timer()
def generate_random_polygons(n: int, 
                             us_boundary: gpd.GeoDataFrame = gpd.read_file('data/conus_buffer.parquet'), 
                             admin_boundaries: gpd.GeoDataFrame = gpd.read_file('data/conus_admin.parquet'), 
                             precision=6) -> gpd.GeoDataFrame:
    """
    Generate n random polygons fully within the contiguous U.S. (CONUS).
    Ensures all polygons are clipped to the CONUS boundary.

    Params:
        n (int): number of polygons to be generated
        us_boundary: a GeoPandas GeoDataframe of the boundary of the US
        admin_boundaries: a GeoPandas GeoDataframe of the administrative information of the US

    Returns:
        gdf: a GeoPandas GeoDataframe of the polygon layer with administrative and geohash information.
    """
    if us_boundary is None or admin_boundaries is None:
        raise ValueError("Both U.S. boundary and administrative boundaries are required.")

    # Compute bounding box of CONUS
    minx, miny, maxx, maxy = us_boundary.total_bounds

    polygons = []
    ids = []

    while len(polygons) < n:
        # Generate random coordinates within the bounding box
        x1, y1 = random.uniform(minx, maxx), random.uniform(miny, maxy)
        x2, y2 = x1 + random.uniform(0.01, 0.1), y1 + random.uniform(0.01, 0.1)
        polygon = box(x1, y1, x2, y2)

        # Ensure the polygon is fully within the CONUS boundary
        if polygon.within(us_boundary.geometry.iloc[0]):  # Checks containment
            polygons.append(polygon)
            ids.append(str(uuid.uuid4()))

    # Create GeoDataFrame
    gdf = gpd.GeoDataFrame(geometry=polygons, crs="EPSG:4326")
    gdf["id"] = ids

    # Clip to CONUS boundary (extra safeguard)
    gdf = gpd.clip(gdf, us_boundary)

    # Compute representative points for spatial join
    gdf["rep_point"] = gdf.geometry.representative_point()

    # Perform spatial join to get city, county, state
    gdf = gdf.sjoin(admin_boundaries, how="left", predicate="within")

    # Compute geohash for location
    gdf["geohash"] = gdf["rep_point"].apply(
        lambda geom: geohash.encode(geom.y, geom.x, precision) if geom else None
    )

    # Drop temporary columns
    gdf = gdf.drop(columns=["rep_point", "index_right"])

    # Drop other columns
    cols_to_keep = [
        'geometry', 
        'id',
        # 'shapeName', 
        # 'STATEFP', 
        # 'COUNTYFP', 
        # 'GEOID', 
        # 'NAME', 
        # 'NAMELSAD', 
        # 'area_fips', 
        'geohash']
    
    # save metadata
    meta_df = gdf[[
        'id',
        'shapeName', 
        'STATEFP', 
        'COUNTYFP', 
        'GEOID', 
        'NAME', 
        'NAMELSAD', 
        'area_fips']]

    # save gdf with dropped cols
    gdf = gdf[cols_to_keep]

    return gdf, meta_df

# @timing_decorator

@Timer()
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

# @timing_decorator
@Timer()
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

# @timing_decorator
@Timer()
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

# @timing_decorator
@Timer()
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

    # Enable pgcrypto extension only once (for UUID generation)
    cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
    conn.commit()

    # Securely create the table using sql.Identifier
    create_table_query = (sql.SQL("""
        DROP TABLE IF EXISTS {};
        CREATE TABLE {} (
            geometry TEXT,
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            geohash TEXT
        );
    """).format(sql.Identifier(table_name), sql.Identifier(table_name)))

    cur.execute(create_table_query)
    conn.commit()

    # Optionally truncate the table
    if truncate:
        truncate_query = sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(table_name))
        cur.execute(truncate_query)
        conn.commit()
        print(f"Table {table_name} truncated.")

    # Insert data if available
    if data:
        insert_query = sql.SQL("""
            INSERT INTO {} (geometry, id, geohash) 
            VALUES (%s, %s, %s);
        """).format(sql.Identifier(table_name))
    # INSERT INTO {} (geometry, id, shapename, statefp, countyfp, geoid, name, namelsad, area_fips, geohash) 
    # VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);

        cur.executemany(insert_query, data)
        conn.commit()
        print(f"Inserted {len(data)} records into {table_name}.")
    else:
        print(f"No data provided for {table_name}.")

    cur.close()
    conn.close()

@Timer()
def retrieve_pg_table(postgresql_details: dict = None, db_name: str = 'blob_matching', table_name: str = '', log_enabled=True):
    """
    Retrieves data from a PostgreSQL table and converts WKT back to geometries.
    """
    if postgresql_details is None:
        postgresql_details = pg_details()
    
    if not table_name:
        raise ValueError("Table name must be specified.")

    postgresql_details['dbname'] = db_name
    conn = psycopg2.connect(**postgresql_details)
    cur = conn.cursor()

    # Retrieve data
    cur.execute(sql.SQL(f"SELECT geometry, id, geohash FROM {table_name};"))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    # Convert to DataFrame & reapply geometry
    df = pd.DataFrame(rows, columns=["geometry", "id", "geohash"])
    df["geometry"] = df["geometry"].apply(loads)  # Convert WKT to Shapely geometry

    if log_enabled:
        print(f"Retrieved {len(df)} records from {table_name}.")

    return df

# def process_batch(geohash_chunk, table_prev, table_curr, postgresql_details, db_name, output_table):
#     """Process a batch of geohashes"""
    
#     df_prev = retrieve_pg_table(postgresql_details, db_name, table_prev, log_enabled=False)
#     df_curr = retrieve_pg_table(postgresql_details, db_name, table_curr, log_enabled=False)

#     # Filter by geohash
#     df_prev = df_prev[df_prev['geohash'].isin(geohash_chunk)]
#     df_curr = df_curr[df_curr['geohash'].isin(geohash_chunk)]

#     if df_prev.empty or df_curr.empty:
#         return

#     matched_pairs = match_geometries(df_prev, df_curr)

#     # Store results in database
#     if matched_pairs:
#         conn = psycopg2.connect(**postgresql_details)
#         cur = conn.cursor()
#         insert_query = sql.SQL(f"INSERT INTO {output_table} (prev_id, curr_id) VALUES %s")
#         extras.execute_values(cur, insert_query, matched_pairs)
#         conn.commit()
#         cur.close()
#         conn.close()

# class SimpleTimer:
#     """Lightweight timer for logging execution time across multiple processes without duplicating log sessions."""

#     _initialized = False  # Prevent multiple initializations

#     def __init__(self, log_to_console=True, log_to_file=True, track_resources=True):
#         if SimpleTimer._initialized:
#             return  # Prevent re-initialization
#         SimpleTimer._initialized = True

#         self.log_to_console = log_to_console
#         self.log_to_file = log_to_file
#         self.track_resources = track_resources

#         # ✅ Use a single session ID across all processes
#         if not hasattr(SimpleTimer, "session_id"):
#             SimpleTimer.session_id = f"sid_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"  

#         # ✅ Use shared log file paths
#         self.log_dir = "logs"
#         os.makedirs(self.log_dir, exist_ok=True)

#         self.RESULTS_FILE = os.path.join(self.log_dir, f"{SimpleTimer.session_id}_timing_results.csv")
#         self.LOG_FILE = os.path.join(self.log_dir, f"{SimpleTimer.session_id}_timing.log")

#         # ✅ Only initialize logging in the main process
#         if mp.current_process().name == "MainProcess":
#             self._ensure_files_exist()
#             self._setup_logging()

#     def _ensure_files_exist(self):
#         """Ensure CSV and log files exist (only in the main process)."""
#         if not os.path.exists(self.RESULTS_FILE):
#             with open(self.RESULTS_FILE, mode="w", newline="") as file:
#                 writer = csv.writer(file)
#                 writer.writerow(["Session ID", "Timestamp", "Function Name", "Execution Time (s)", "CPU Usage (%)", "Memory Usage (MB)", "Arguments"])
#             print(f"Created {self.RESULTS_FILE}")

#         if not os.path.exists(self.LOG_FILE):
#             open(self.LOG_FILE, "w").close()  # Create empty file
#             print(f"Created {self.LOG_FILE}")

#     def _setup_logging(self):
#         """Set up logging only once and ensure all processes log to the same file."""
#         logging.shutdown()  # Close previous handlers (if any)
#         for handler in logging.root.handlers[:]:
#             logging.root.removeHandler(handler)

#         logging.basicConfig(
#             filename=self.LOG_FILE,
#             level=logging.INFO,
#             format="%(asctime)s - %(levelname)s - %(message)s",
#             filemode="a",  # ✅ Append to the same log file
#         )

#     def __call__(self, func):
#         """Decorator to time a function and log results."""
#         @functools.wraps(func)
#         def wrapper(*args, **kwargs):
#             start_time = time.time()
#             cpu_start = psutil.cpu_percent(interval=None) if self.track_resources else None
#             mem_start = psutil.virtual_memory().used / (1024 ** 2) if self.track_resources else None  # Convert to MB

#             result = func(*args, **kwargs)

#             elapsed_time = time.time() - start_time
#             cpu_end = psutil.cpu_percent(interval=None)
#             mem_end = psutil.virtual_memory().used / (1024 ** 2)

#             timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#             args_repr = json.dumps({"args": args, "kwargs": kwargs}, default=str)  

#             cpu_usage = abs(cpu_end - cpu_start) if cpu_start is not None else None
#             mem_usage = abs(mem_end - mem_start) if mem_start is not None else None

#             log_message = f"[{timestamp}] Function `{func.__name__}` completed in {elapsed_time:.4f} sec, CPU: {cpu_usage:.2f}%, Memory: {mem_usage:.2f}MB"

#             if self.log_to_console and mp.current_process().name == "MainProcess":
#                 print(log_message)
#             logging.info(log_message)

#             if self.log_to_file:
#                 self._save_to_csv(timestamp, func.__name__, elapsed_time, cpu_usage, mem_usage, args_repr)

#             return result

#         return wrapper

#     def _save_to_csv(self, timestamp, function_name, elapsed_time, cpu_usage, mem_usage, args_repr):
#         """Append results to the same CSV file."""
#         with open(self.RESULTS_FILE, mode="a", newline="") as file:
#             writer = csv.writer(file)
#             writer.writerow([SimpleTimer.session_id, timestamp, function_name, elapsed_time, cpu_usage, mem_usage, args_repr])

# # ✅ Create a single global instance to ensure one-time initialization
# simple_timing_decorator = SimpleTimer(log_to_console=True, log_to_file=True, track_resources=True)

# @simple_timing_decorator  # Only timing the main function
# def run_parallel_matching(table_prev, table_curr, output_table, postgresql_details, db_name, num_workers=4, batch_size=100):
#     """Parallel processing of geohash chunks with worker limit"""
#     df_prev = retrieve_pg_table(postgresql_details, db_name, table_prev)

#     geohashes = df_prev["geohash"].dropna().unique().tolist()
#     chunks = [geohashes[i:i + batch_size] for i in range(0, len(geohashes), batch_size)]

#     processes = []
#     for chunk in chunks:
#         # Limit the number of concurrent processes to num_workers
#         while len(processes) >= num_workers:
#             for p in processes:
#                 p.join(timeout=0.1)  # Check if process is finished
#             processes = [p for p in processes if p.is_alive()]  # Remove completed processes

#         p = mp.Process(target=process_batch, args=(chunk, table_prev, table_curr, postgresql_details, db_name, output_table))
#         p.start()
#         processes.append(p)

#     # Ensure all processes finish
#     for p in processes:
#         p.join()

# def track_execution_time(func, *args, **kwargs):
#     """
#     Manually track execution time for the entire run_parallel_matching function.
#     Ensures logging happens only once.
#     """
#     # Start tracking
#     start_time = time.time()
#     cpu_start = psutil.cpu_percent(interval=None)
#     mem_start = psutil.virtual_memory().used / (1024 ** 2)  # MB

#     # Execute function
#     result = func(*args, **kwargs)

#     # Stop tracking
#     elapsed_time = time.time() - start_time
#     cpu_end = psutil.cpu_percent(interval=None)
#     mem_end = psutil.virtual_memory().used / (1024 ** 2)  # MB

#     timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     cpu_usage = abs(cpu_end - cpu_start)
#     mem_usage = abs(mem_end - mem_start)

#     log_message = f"[{timestamp}] Function `{func.__name__}` completed in {elapsed_time:.4f} sec, CPU: {cpu_usage:.2f}%, Memory: {mem_usage:.2f}MB"

#     print(log_message)
#     logging.info(log_message)

#     return result

# def run_matching_with_timing(table_prev, table_curr, output_table, postgresql_details, db_name, num_workers=4, batch_size=100):
#     """
#     Wrapper function to track execution time and call run_parallel_matching.
#     """
#     return track_execution_time(run_parallel_matching, table_prev, table_curr, output_table, postgresql_details, db_name, num_workers, batch_size)

# def logger_process(log_queue):
#     """Process that handles logging from multiple workers."""
#     logger = logging.getLogger("multiprocessing_logger")
#     logger.setLevel(logging.INFO)
    
#     handler = logging.FileHandler(LOG_FILE, mode="a")
#     formatter = logging.Formatter('%(asctime)s - %(processName)s - %(levelname)s - %(message)s')
#     handler.setFormatter(formatter)
#     logger.addHandler(handler)

#     while True:
#         record = log_queue.get()
#         if record is None:  # Stop signal
#             break
#         logger.handle(record)

# def setup_worker_logger(log_queue):
#     """Sets up a worker process to send logs to the logging queue."""
#     queue_handler = logging.handlers.QueueHandler(log_queue)
#     logger = logging.getLogger("multiprocessing_logger")
#     logger.setLevel(logging.INFO)
#     logger.addHandler(queue_handler)
#     return logger

# def track_execution_time(func, log_queue, *args, **kwargs):
#     """Tracks execution time, CPU, and memory usage in multiprocessing."""
#     start_time = time.time()
#     cpu_start = psutil.cpu_percent(interval=None)
#     mem_start = psutil.virtual_memory().used / (1024 ** 2)

#     result = func(*args, **kwargs)

#     elapsed_time = time.time() - start_time
#     cpu_end = psutil.cpu_percent(interval=None)
#     mem_end = psutil.virtual_memory().used / (1024 ** 2)

#     cpu_usage = abs(cpu_end - cpu_start)
#     mem_usage = abs(mem_end - mem_start)

#     log_message = f"Function `{func.__name__}` completed in {elapsed_time:.4f} sec, CPU: {cpu_usage:.2f}%, Memory: {mem_usage:.2f}MB"

#     log_queue.put(logging.LogRecord(
#         name="multiprocessing_logger",
#         level=logging.INFO,
#         pathname="",
#         lineno=0,
#         msg=log_message,
#         args=None,
#         exc_info=None
#     ))

#     # ✅ Append results to a shared CSV file safely
#     with open(CSV_FILE, mode="a", newline="") as file:
#         writer = csv.writer(file)
#         writer.writerow([SESSION_ID, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), func.__name__, elapsed_time, cpu_usage, mem_usage])

#     return result

# def run_parallel_matching(table_prev, table_curr, output_table, postgresql_details, db_name, num_workers=4, batch_size=100, log_queue=None):
#     """Parallel processing of geohash chunks with a shared logging queue."""
#     df_prev = retrieve_pg_table(postgresql_details, db_name, table_prev)
#     geohashes = df_prev["geohash"].dropna().unique().tolist()
#     chunks = [geohashes[i:i + batch_size] for i in range(0, len(geohashes), batch_size)]

#     processes = []
#     for chunk in chunks:
#         while len(processes) >= num_workers:
#             for p in processes:
#                 p.join(timeout=0.1)
#             processes = [p for p in processes if p.is_alive()]

#         p = mp.Process(target=track_execution_time, args=(process_batch, log_queue, chunk, table_prev, table_curr, postgresql_details, db_name, output_table))
#         p.start()
#         processes.append(p)

#     for p in processes:
#         p.join()

def create_matched_results_table(postgresql_details: dict, db_name: str, table_name: str):
    """
    Drops the matched_results table if it exists and creates a new one.
    """
    postgresql_details['dbname'] = db_name
    conn = psycopg2.connect(**postgresql_details)
    cur = conn.cursor()
    
    create_table_query = sql.SQL("""
        DROP TABLE IF EXISTS {};
        CREATE TABLE {} (
            prev_id UUID,
            curr_id UUID
        );
    """).format(sql.Identifier(table_name), sql.Identifier(table_name))
    
    cur.execute(create_table_query)
    conn.commit()
    cur.close()
    conn.close()
    print(f"Table {table_name} created successfully.")

def _retrieve_pg_table(postgresql_details: dict = None, db_name: str = 'blob_matching', table_name: str = '', log_enabled=True):
    """
    Hidden function version for multiprocessing without the logging.
    Retrieves data from a PostgreSQL table and converts WKT back to geometries.
    """
    if postgresql_details is None:
        postgresql_details = pg_details()
    
    if not table_name:
        raise ValueError("Table name must be specified.")

    postgresql_details['dbname'] = db_name
    conn = psycopg2.connect(**postgresql_details)
    cur = conn.cursor()

    # Retrieve data
    cur.execute(sql.SQL(f"SELECT geometry, id, geohash FROM {table_name};"))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    # Convert to DataFrame & reapply geometry
    df = pd.DataFrame(rows, columns=["geometry", "id", "geohash"])
    df["geometry"] = df["geometry"].apply(loads)  # Convert WKT to Shapely geometry

    if log_enabled:
        print(f"Retrieved {len(df)} records from {table_name}.")

    return df

# Helper function to handle multiprocessing actions
def worker_logger(logger, cpu_start, mem_start):
    """Helper function to log CPU and memory usage for multiprocessing."""
    if logger:
        cpu_end = psutil.cpu_percent(interval=None)
        mem_end = psutil.virtual_memory().used / (1024 ** 2)  # MB

        elapsed_time = time.time() - cpu_start
        log_message = f"Process completed in {elapsed_time:.4f} seconds, CPU: {cpu_end:.2f}%, Memory: {mem_end:.2f}MB"
        logger.info(log_message)

@Timer()
def match_geometries(df_prev, df_curr):
    """
    Match geometries using Shapely intersection or equality.
    This makes it an O(n * m) operation, 
        where n is the number of rows in df_prev and m is in df_curr.
    """
    matched = []

    # Explicitly log function entry
    logging.info("Starting match_geometries()")

    for _, row1 in df_prev.iterrows():
        for _, row2 in df_curr.iterrows():
            if row1.geometry.intersects(row2.geometry):  # Using intersects() instead of equals()
                matched.append((row1.id, row2.id))
    
    # Add logging to confirm whether matches are found
    logging.info(f"match_geometries: Found {len(matched)} matches")
    
    return matched

def get_neighbors(ghash, precision=7):
    """Compute the 8 neighboring geohashes."""
    lat, lon = geohash.decode(ghash)  # Decode geohash into latitude & longitude
    lat, lon = float(lat), float(lon)  # 🔧 Ensure values are floats

    delta = 10 ** (-precision)  # Small shift based on precision

    neighbors = [
        geohash.encode(lat + dy * delta, lon + dx * delta, precision)
        for dx, dy in itertools.product([-1, 0, 1], repeat=2)
        if not (dx == 0 and dy == 0)  # Exclude the center geohash itself
    ]
    return neighbors

# Applying Timer to process_batch to track each batch's start and end time
# @Timer(log_to_console=True, log_to_file=True, track_resources=True)
def process_batch(geohash_chunk, table_prev, table_curr, postgresql_details, db_name, output_table, logger):
    """Process a batch of geohashes"""
    
    df_prev = _retrieve_pg_table(postgresql_details, db_name, table_prev, log_enabled=False)
    df_curr = _retrieve_pg_table(postgresql_details, db_name, table_curr, log_enabled=False)

    # Log the number of polygons for this geohash chunk
    logging.info(f"Chunk {geohash_chunk[:3]}...: {len(df_prev)} prev, {len(df_curr)} curr polygons")

    # Filter by geohash, including neighboring ones
    # df_prev = df_prev[df_prev['geohash'].isin(geohash_chunk)]
    # df_curr = df_curr[df_curr['geohash'].isin(geohash_chunk)]
    neighboring_geohashes = set()
    for g in geohash_chunk:
        neighboring_geohashes.add(g)
        neighboring_geohashes.update(get_neighbors(g))  # Get 8 adjacent geohashes

    df_prev = df_prev[df_prev['geohash'].isin(neighboring_geohashes)]
    df_curr = df_curr[df_curr['geohash'].isin(neighboring_geohashes)]

    logging.info(f'After filtering: {len(df_prev)} prev, {len(df_curr)} curr polygons')

    if df_prev.empty or df_curr.empty:
        logging.info('Skipping batch - No overlapping geohashes')
        return

    matched_pairs = match_geometries(df_prev, df_curr)
    logging.info(f"Found {len(matched_pairs)} matches in chunk {geohash_chunk[:3]}...")

    # Store results in database
    if matched_pairs:
        logging.info(f"Inserting {len(matched_pairs)} matches into {output_table}")
        logging.info(f"Sample match: {matched_pairs[:5]}")  # Log a few matches
        conn = psycopg2.connect(**postgresql_details)
        cur = conn.cursor()
        insert_query = sql.SQL(f"INSERT INTO {output_table} (prev_id, curr_id) VALUES %s")
        try:
            extras.execute_values(cur, insert_query, matched_pairs)
            conn.commit()
        except Exception as e:
            logging.error(f'Database insert failed: {e}')
            conn.rollback()
        finally:
            cur.close()
            conn.close()

# Main multiprocessing function with Timer applied
@Timer(log_to_console=True, log_to_file=True, track_resources=True)
def run_parallel_matching(table_prev, table_curr, output_table, postgresql_details, db_name, num_workers=4, batch_size=100):
    """Parallel processing of geohash chunks with worker limit"""
    
    # Create (or recreate) the matched_results table
    create_matched_results_table(postgresql_details, db_name, output_table)

    manager = mp.Manager()
    logger = manager.list()  # Shared memory list to store logs

    df_prev = _retrieve_pg_table(postgresql_details, db_name, table_prev)
    geohashes = df_prev["geohash"].dropna().unique().tolist()
    chunks = [geohashes[i:i + batch_size] for i in range(0, len(geohashes), batch_size)]

    processes = []
    cpu_start = time.time()  # Start the overall time tracker

    for chunk in chunks:
        # Limit the number of concurrent processes to num_workers
        while len(processes) >= num_workers:
            for p in processes:
                p.join(timeout=0.1)  # Check if process is finished
            processes = [p for p in processes if p.is_alive()]  # Remove completed processes

        p = mp.Process(target=process_batch, args=(chunk, table_prev, table_curr, postgresql_details, db_name, output_table, logger))
        p.start()
        processes.append(p)

    # Ensure all processes finish
    for p in processes:
        p.join()

    # Log overall CPU and memory usage after all processes complete
    worker_logger(logger, cpu_start, None)  # You can replace `None` with memory tracking logic if needed



