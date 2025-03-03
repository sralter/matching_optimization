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
from multiprocessing import Value, Lock, Pool, Manager
import os
import math
from typing import Literal

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

@Timer()
def generate_random_polygons(n: int, 
                             us_boundary: gpd.GeoDataFrame = gpd.read_file('data/conus_buffer.parquet'), 
                             admin_boundaries: gpd.GeoDataFrame = gpd.read_file('data/conus_admin.parquet'), 
                            #  precision=7) -> gpd.GeoDataFrame:
                             precision=8) -> gpd.GeoDataFrame:
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
        # x2, y2 = x1 + random.uniform(0.0001, 0.001), y1 + random.uniform(0.0001, 0.001)
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
def retrieve_pg_table(postgresql_details: dict = None, db_name: str = 'blob_matching', table_name: str = '', log_enabled: bool = True, count: bool = False):
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

    if not count:
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
    
    # Retrieve count of matches
    cur.execute(sql.SQL(f"SELECT COUNT(*) FROM {table_name};"))
    match_count = cur.fetchone()[0]  # Extract count

    cur.close()
    conn.close()

    if log_enabled:
        print(f"Total matches in {table_name}: {match_count}")

    return match_count

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

def _retrieve_pg_table(postgresql_details: dict = None, db_name: str = 'blob_matching', table_name: str = '', log_queue=None):
    """
    Hidden function version for multiprocessing without direct logging.
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

    # ✅ Log the retrieved records
    if log_queue:
        log_queue.put(logging.LogRecord(
            name="multiprocessing_logger",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg=f"Retrieved {len(df)} records from {table_name}.",
            args=None,
            exc_info=None
        ))

    return df

# Helper function to handle multiprocessing actions
def worker_logger(start_time, memory_start, log_queue):
    """Logs final CPU, memory, and execution time at the end of processing."""
    elapsed_time = time.time() - start_time
    cpu_usage = psutil.cpu_percent(interval=None)
    memory_end = psutil.virtual_memory().used / (1024 ** 2)
    memory_usage = abs(memory_end - memory_start) if memory_start else None

    log_queue.put(logging.LogRecord(
        name="multiprocessing_logger",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg=f"Total Execution Time: {elapsed_time:.4f} sec, CPU Usage: {cpu_usage:.2f}%, Memory Usage: {memory_usage:.2f}MB",
        args=None,
        exc_info=None
    ))

    # Optionally, append to CSV here if needed.

# @Timer()
def match_geometries(df_prev, df_curr, log_queue, match_count, lock):
    """
    Match geometries using Shapely intersection or equality.
    This makes it an O(n * m) operation, 
        where n is the number of rows in df_prev and m is in df_curr.
    Pre-filtering by geohash will help speed things up.
    """
    matched = []
    total = len(df_prev)
    last_report = 0

    log_queue.put(logging.LogRecord(
        name="multiprocessing_logger",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="Starting match_geometries()",
        args=None,
        exc_info=None
    ))

    for i, (_, row1) in enumerate(df_prev.iterrows(), start=1):
        for _, row2 in df_curr.iterrows():
            if row1.geometry.intersects(row2.geometry):  
                matched.append((row1.id, row2.id))
                with lock:
                    match_count.value += 1  # Safely update shared counter

        percent = (i / total) * 100
        if percent >= last_report + 10:
            last_report = int(percent // 10) * 10
            if i == 1:  # Only log the first worker's progress
                log_queue.put(logging.LogRecord(
                    name="multiprocessing_logger",
                    level=logging.INFO,
                    pathname="",
                    lineno=0,
                    msg=f"match_geometries: {last_report}% of rows processed, {match_count.value} matches found",
                    args=None,
                    exc_info=None
                ))

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
def process_batch(geohash_chunk, table_prev, table_curr, postgresql_details, db_name, output_table, log_queue, match_count, lock): # logger argument removed
    """Process a batch of geohashes"""
    start_time = time.time()
    memory_start = psutil.virtual_memory().used / (1024 ** 2)  # Capture memory start in MB

    df_prev = _retrieve_pg_table(postgresql_details, db_name, table_prev, log_queue=log_queue)
    df_curr = _retrieve_pg_table(postgresql_details, db_name, table_curr, log_queue=log_queue)

    # Use log_queue instead of direct logging
    log_queue.put(logging.LogRecord(
        name="multiprocessing_logger",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg=f"Chunk {geohash_chunk[:3]}...: {len(df_prev)} prev, {len(df_curr)} curr polygons",
        args=None,
        exc_info=None
    ))

    # Filter by geohash, including neighboring ones
    neighboring_geohashes = set()
    for g in geohash_chunk:
        neighboring_geohashes.add(g)
        neighboring_geohashes.update(get_neighbors(g))  # Get 8 adjacent geohashes

    # testing if this helps or not - comment or uncomment this
    df_prev = df_prev[df_prev['geohash'].isin(neighboring_geohashes)]
    df_curr = df_curr[df_curr['geohash'].isin(neighboring_geohashes)]

    if df_prev.empty or df_curr.empty:
        log_queue.put(logging.LogRecord(
            name="multiprocessing_logger",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Skipping batch - No overlapping geohashes",
            args=None,
            exc_info=None
        ))
        return

    try:
        matched_pairs = match_geometries(df_prev, df_curr, log_queue, match_count, lock)
    except Exception as e:
        log_queue.put(logging.LogRecord(
            name="multiprocessing_logger",
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg=f"Error in match_geometries: {e}",
            args=None,
            exc_info=True
        ))

    log_queue.put(logging.LogRecord(
        name="multiprocessing_logger",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg=f"Found {len(matched_pairs)} matches in chunk {geohash_chunk[:3]}...",
        args=None,
        exc_info=None
    ))

    # Store results in database
    if matched_pairs:
        conn = psycopg2.connect(**postgresql_details)
        cur = conn.cursor()
        insert_query = sql.SQL(f"INSERT INTO {output_table} (prev_id, curr_id) VALUES %s")
        try:
            extras.execute_values(cur, insert_query, matched_pairs)
            conn.commit()
        except Exception as e:
            log_queue.put(logging.LogRecord(
                name="multiprocessing_logger",
                level=logging.ERROR,
                pathname="",
                lineno=0,
                msg=f"Database insert failed: {e}",
                args=None,
                exc_info=None
            ))
            conn.rollback()
        finally:
            cur.close()
            conn.close()

    # Log elapsed time and memory usage after processing the batch
    elapsed_time = time.time() - start_time
    memory_end = psutil.virtual_memory().used / (1024 ** 2)  # Capture memory after batch
    memory_used = abs(memory_end - memory_start)

    log_queue.put(logging.LogRecord(
        name="multiprocessing_logger",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg=f"Chunk {geohash_chunk[:3]} completed in {elapsed_time:.4f} sec, Memory Used: {memory_used:.2f} MB",
        args=None,
        exc_info=None
    ))

def logging_listener(log_queue):
    """Listener function that handles logs coming from multiple processes."""
    while True:
        try:
            record = log_queue.get()
            if record is None:
                break  # Exit when sentinel value is received
            logger = logging.getLogger(record.name)
            logger.handle(record)
        except EOFError:
            print("Log queue closed unexpectedly.")
            break  # Prevents crash if the queue is closed early
        except Exception as e:
            print(f"Logging error: {e}")
    print('Logging listener has shut down.')

# Main multiprocessing function with Timer applied
# @Timer(log_to_console=True, log_to_file=True, track_resources=True)
def run_parallel_matching(table_prev, table_curr, output_table, postgresql_details, db_name, num_workers=4, batch_size=None, verbose=2):
    """Parallel processing of geohash chunks with adaptive batch size and worker limit."""

    manager = Manager()
    log_queue = manager.Queue()
    match_count = manager.Value('i', 0)  # Shared counter for matches
    lock = manager.Lock()

    log_process = mp.Process(target=logging_listener, args=(log_queue,))
    log_process.start()

    create_matched_results_table(postgresql_details, db_name, output_table)

    df_prev = _retrieve_pg_table(postgresql_details, db_name, table_prev, log_queue)
    geohashes = df_prev["geohash"].dropna().unique().tolist()

    if batch_size is None:
        total_geohashes = len(geohashes)
        max_batches = min(num_workers * 4, 100)
        batch_size = max(1000, math.ceil(total_geohashes / max_batches))

    chunks = [geohashes[i:i + batch_size] for i in range(0, len(geohashes), batch_size)]
    total_batches = len(chunks)
    print(f'Total batches to process: {total_batches}')

    # Use a pool for parallel processing
    with Pool(processes=num_workers) as pool:
        pool.starmap(process_batch, [(chunk, table_prev, table_curr, postgresql_details, db_name, output_table, log_queue, match_count, lock) for chunk in chunks])

    # After the pool has finished processing all batches:
    message = f"Total matches found: {match_count.value}"
    print(message)
    log_queue.put(logging.LogRecord(
        name="multiprocessing_logger",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg=message,
        args=None,
        exc_info=None
    ))


    log_queue.put(None)
    log_process.join()
    log_process.terminate()

def convert_logs_to_parquet(csv_path: str, log_path: str, output_dir: str = "logs"):
    """
    Converts timing_results.csv and timing.log into Parquet files for efficient storage.
    
    Args:
        csv_path (str): Path to the CSV file.
        log_path (str): Path to the log file.
        output_dir (str): Directory to store Parquet files.
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # Convert CSV to Parquet
    csv_output_path = os.path.join(output_dir, "timing_results.parquet")
    try:
        df_csv = pd.read_csv(csv_path)
        df_csv.to_parquet(csv_output_path, index=False)
        print(f"Converted {csv_path} to {csv_output_path}")
    except Exception as e:
        print(f"Error converting CSV to Parquet: {e}")
    
    # Convert log file to Parquet
    log_output_path = os.path.join(output_dir, "timing_log.parquet")
    try:
        with open(log_path, "r") as log_file:
            log_lines = log_file.readlines()
        
        df_log = pd.DataFrame({"log_entry": log_lines})
        df_log.to_parquet(log_output_path, index=False)
        print(f"Converted {log_path} to {log_output_path}")
    except Exception as e:
        print(f"Error converting log file to Parquet: {e}")

@Timer()
def compute_h3_indices(geometry, centroid_res=6, polyfill_res=9):
    """
    Compute H3 indices for a polygon:
    - Single H3 index based on centroid
    - Full polygon coverage with H3 polyfill at high resolution
    """
    centroid = geometry.centroid
    h3_centroid = h3.geo_to_h3(centroid.y, centroid.x, centroid_res)

    # Full coverage using polyfill
    h3_polyfill = list(h3.polyfill(geometry.__geo_interface__, polyfill_res))

    return h3_centroid, h3_polyfill

