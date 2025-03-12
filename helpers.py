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
from shapely import to_wkt, wkt
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
import h3
import duckdb
import pyiceberg
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.io.pyarrow import PyArrowFileIO
import pandas as pd
from psycopg2 import sql
from pathlib import Path
from sqlalchemy import create_engine
import sqlite3

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
            def safe_serialize(obj):
                """Safely convert args/kwargs for logging, handling DataFrames."""
                if isinstance(obj, (pd.DataFrame, gpd.GeoDataFrame)):
                    return f"<DataFrame with {len(obj)} rows>"
                return str(obj)
            # Remove default=str to avoid fallback conversion that triggers __nonzero__
            args_repr = json.dumps(
                {"args": [safe_serialize(arg) for arg in args],
                "kwargs": {k: safe_serialize(v) for k, v in kwargs.items()}}
            )

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
                             us_boundary: gpd.GeoDataFrame = gpd.read_file('archive/conus_buffer.parquet'), 
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

# @Timer()
# def convert_col_to_string(df: gpd.GeoDataFrame, col: str = 'geometry') -> gpd.GeoDataFrame:
#     """
#     Converts a specified geometry column to a WKT string representation.

#     Args:
#         df (gpd.GeoDataFrame): The GeoDataFrame with the geometry column.
#         col (str): The column that will be converted, defaults to 'geometry'.

#     Returns:
#         gpd.GeoDataFrame: A new GeoDataFrame with the converted column.
#     """
#     df = df.copy()  # Ensure we don't modify the original dataframe
#     df[col] = df[col].apply(lambda geom: to_wkt(geom) if isinstance(geom, BaseGeometry) else str(geom))
#     return df
@Timer()
def convert_geom_columns_to_string(df: gpd.GeoDataFrame, geom_columns: list = None) -> gpd.GeoDataFrame:
    """
    Converts geometry-like columns in a GeoDataFrame to their WKT string representations.
    
    If no geom_columns list is provided, the function automatically detects columns
    containing shapely geometry objects by checking the first non-null value in each column.
    
    Args:
        df (gpd.GeoDataFrame): Input GeoDataFrame.
        geom_columns (list, optional): List of column names to convert. If None,
            the function will detect columns with geometry-like data.
    
    Returns:
        gpd.GeoDataFrame: Modified GeoDataFrame with converted columns.
    """
    from shapely.geometry.base import BaseGeometry  # Ensure BaseGeometry is imported
    
    df = df.copy()
    # Automatically detect geometry-like columns if not provided
    if geom_columns is None:
        geom_columns = []
        for col in df.columns:
            non_null = df[col].dropna()
            if not non_null.empty:
                first_value = non_null.iloc[0]
                if isinstance(first_value, BaseGeometry):
                    geom_columns.append(col)
    
    # Convert the detected geometry-like columns to WKT strings
    for col in geom_columns:
        if col in df.columns:
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

@Timer()
def generate_pg_schema(df: pd.DataFrame) -> str:
    """
    Generate a PostgreSQL table schema from a pandas DataFrame.
    
    The function maps common pandas dtypes to PostgreSQL types.
    All column names are converted to lower-case to match PostgreSQL's default behavior.
    
    Args:
        df (pd.DataFrame): The input DataFrame.
        
    Returns:
        schema (str): A string with the column definitions for PostgreSQL.
    """
    # Mapping from pandas dtypes to PostgreSQL types
    type_mapping = {
        'object': 'TEXT',                  # For string and generic objects
        'int64': 'INTEGER',
        'float64': 'DOUBLE PRECISION',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP'
    }
    
    columns_schema = []
    for col, dtype in df.dtypes.items():
        dtype_str = str(dtype)
        pg_type = type_mapping.get(dtype_str, 'TEXT')  # Default to TEXT if type not found

        # Optionally, if the column name is "id" (case insensitive) we set it as a UUID PK.
        if col.lower() == 'id':
            column_def = f"{col.lower()} UUID PRIMARY KEY DEFAULT gen_random_uuid()"
        else:
            column_def = f"{col.lower()} {pg_type}"
        columns_schema.append(column_def)
    
    # Join all column definitions with commas
    schema = ",\n".join(columns_schema)
    return schema

@Timer()
def create_pg_table(postgresql_details: dict = None, 
                    db_name: str = 'blob_matching', 
                    table_name: str = '', 
                    data: pd.DataFrame = None, 
                    truncate: bool = False):
    """
    Creates a PostgreSQL table based on the DataFrame's schema and inserts the data.

    This function dynamically generates the CREATE TABLE statement based on
    the input DataFrame's columns and automatically converts the DataFrame into
    a list of tuples for insertion.

    Args:
        postgresql_details (dict): Connection details for PostgreSQL.
        db_name (str): Name of the target database.
        table_name (str): Name of the table to create.
        data (pd.DataFrame): Input DataFrame containing the data.
        truncate (bool): If True, truncates the table after creating it.
    """
    if postgresql_details is None:
        postgresql_details = pg_details()  # Assuming you have a function that returns these details
    
    if not table_name:
        raise ValueError("Table name must be specified.")
    
    if data is None:
        data = pd.DataFrame()  # Use an empty DataFrame if none provided
    
    # Connect to the database
    postgresql_details['dbname'] = db_name
    conn = psycopg2.connect(**postgresql_details)
    cur = conn.cursor()
    
    # Enable the pgcrypto extension (needed for gen_random_uuid()) if not already enabled
    cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
    conn.commit()
    
    # Generate table schema from the DataFrame
    schema_str = generate_pg_schema(data)
    
    # Securely create the table using the generated schema
    create_table_query = sql.SQL("""
        DROP TABLE IF EXISTS {};
        CREATE TABLE {} (
            {}
        );
    """).format(
        sql.Identifier(table_name),
        sql.Identifier(table_name),
        sql.SQL(schema_str)
    )
    cur.execute(create_table_query)
    conn.commit()
    
    # Optionally truncate the table
    if truncate:
        truncate_query = sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(table_name))
        cur.execute(truncate_query)
        conn.commit()
        print(f"Table {table_name} truncated.")
    
    # debug
    # print(f"DEBUG: Type of data received: {type(data)}")
    # print(f"DEBUG: Data is empty? {data.empty if isinstance(data, pd.DataFrame) else 'N/A'}")

    # Insert data if available
    if not data.empty:
        # Convert DataFrame rows to a list of tuples (internal conversion)
        data_tuples = list(data.itertuples(index=False, name=None))
        
        # # Create an INSERT statement dynamically based on DataFrame columns
        # columns = list(data.columns)
        # placeholders = ", ".join(["%s"] * len(columns))
        # insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        #     sql.Identifier(table_name),
        #     sql.SQL(", ").join(map(sql.Identifier, columns)),
        #     sql.SQL(placeholders)
        # )
        # Create an INSERT statement dynamically based on DataFrame columns
        columns = [col.lower() for col in data.columns]
        placeholders = ", ".join(["%s"] * len(columns))
        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(placeholders)
        )
        cur.executemany(insert_query, data_tuples)
        conn.commit()
        print(f"Inserted {len(data_tuples)} records into {table_name}.")
    else:
        print(f"No data provided for {table_name}.")
    
    cur.close()
    conn.close()

def is_wkt_geometry(value):
    """
    Checks if a value (string) looks like a WKT geometry.
    """
    if isinstance(value, str):
        value = value.strip().upper()
        for prefix in ["POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION"]:
            if value.startswith(prefix):
                return True
    return False

@Timer()
def retrieve_pg_table(postgresql_details: dict = None, 
                      db_name: str = 'blob_matching', 
                      table_name: str = '', 
                      log_enabled: bool = True, 
                      count: bool = False):
    """
    Retrieves data from a PostgreSQL table and converts any WKT strings back to geometries.
    If count is False, all columns are selected dynamically.
    """
    if postgresql_details is None:
        postgresql_details = pg_details()
    
    if not table_name:
        raise ValueError("Table name must be specified.")

    postgresql_details['dbname'] = db_name
    conn = psycopg2.connect(**postgresql_details)
    cur = conn.cursor()

    if not count:
        # Retrieve all columns dynamically
        cur.execute(sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name)))
        rows = cur.fetchall()
        # Extract column names from the cursor description
        columns = [desc[0] for desc in cur.description]
        
        cur.close()
        conn.close()

        # Convert to DataFrame
        df = pd.DataFrame(rows, columns=columns)

        # Automatically detect and convert any column with WKT geometry strings
        for col in df.columns:
            non_null = df[col].dropna()
            if not non_null.empty:
                first_val = non_null.iloc[0]
                if is_wkt_geometry(first_val):
                    # Convert entire column from WKT to a Shapely geometry
                    df[col] = df[col].apply(loads)
                    
        if log_enabled:
            print(f"Retrieved {len(df)} records from {table_name}.")

        return df
    
    # If count=True, retrieve just the count of records.
    cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name)))
    match_count = cur.fetchone()[0]

    cur.close()
    conn.close()

    if log_enabled:
        print(f"Total matches in {table_name}: {match_count}")

    return match_count

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
    # Compute centroid H3 index
    centroid = geometry.centroid
    h3_centroid = h3.latlng_to_cell(centroid.y, centroid.x, centroid_res)

    # Convert exterior coordinates to lists (not tuples)
    coords = [[list(coord) for coord in geometry.exterior.coords]]
    geojson = {
        "type": "Polygon",
        "coordinates": coords
    }

    # Compute full polygon coverage using polygon_to_cells
    h3_polyfill = list(h3.polygon_to_cells(geojson, polyfill_res))

    return h3_centroid, h3_polyfill

def compute_h3_centroid_udf(geom_wkt: str, centroid_res: int = 6) -> str:
    # Convert the WKT string back to a geometry
    geometry = wkt.loads(geom_wkt)
    centroid = geometry.centroid
    return h3.latlng_to_cell(centroid.y, centroid.x, centroid_res)

def compute_h3_polyfill_udf(geom_wkt: str, polyfill_res: int = 9) -> str:
    # Convert the WKT string back to a Shapely geometry.
    geometry = wkt.loads(geom_wkt)
    
    # Get the __geo_interface__; this returns coordinates in (lng, lat) order.
    geo_interface = geometry.__geo_interface__
    
    # Function to swap coordinate order from (lng, lat) to (lat, lng)
    def swap_coords(coords):
        return [[list((pt[1], pt[0])) for pt in ring] for ring in coords]
    
    # Build a new GeoJSON polygon with swapped coordinates.
    corrected_geojson = {
        "type": geo_interface["type"],
        "coordinates": swap_coords(geo_interface["coordinates"])
    }
    
    # Convert the corrected GeoJSON polygon to an H3Shape.
    h3shape = h3.geo_to_h3shape(corrected_geojson)
    
    # Get the H3 cells covering the polygon at the specified resolution.
    polyfill = list(h3.h3shape_to_cells(h3shape, polyfill_res))
    
    # Return the polyfill as a string (or you could return a JSON string or list).
    return str(polyfill)
# def compute_h3_polyfill_udf(geom_wkt: str, polyfill_res: int = 9) -> str:
#     # Convert the WKT string back to a geometry
#     geometry = wkt.loads(geom_wkt)
#     # Convert exterior coordinates from tuples to lists
#     coords = [[list(coord) for coord in geometry.exterior.coords]]
#     geojson = {"type": "Polygon", "coordinates": coords}
#     polyfill = list(h3.polygon_to_cells(geojson, polyfill_res))
#     # Return the polyfill as a JSON string (or any format you prefer)
#     return str(polyfill)

# @Timer()
# # Worker function that processes a single batch (DataFrame chunk)
# def process_h3_info_chunk(df_chunk: pd.DataFrame) -> pd.DataFrame:
#     # Each worker must create its own DuckDB connection
#     con = duckdb.connect()
#     # Register the UDFs
#     con.create_function('compute_h3_centroid', compute_h3_centroid_udf)
#     con.create_function('compute_h3_polyfill', compute_h3_polyfill_udf)
#     # Register the batch DataFrame as a temporary table (name can be fixed since each worker has its own connection)
#     con.register('df_chunk', df_chunk)
#     # Execute the query
#     result = con.execute('''
#         SELECT
#             *,
#             compute_h3_centroid(geometry_wkt, 6) AS h3_centroid,
#             compute_h3_polyfill(geometry_wkt, 9) AS h3_polyfill
#         FROM df_chunk
#     ''').fetch_df()
#     con.close()
#     return result

@Timer()  # Time the overall parallel processing
def create_h3_info_parallel(df_prev: pd.DataFrame, df_curr: pd.DataFrame, num_workers: int = 4) -> tuple:
    """
    Computes H3 centroids and polyfill in parallel using 4 workers.
    
    Each DataFrame is preprocessed (WKT conversion, drop geometry), split into 4 batches,
    and processed in parallel via DuckDB UDFs.
    
    Returns:
        tuple: (df_prev_processed, df_curr_processed)
    """
    # Create WKT columns (DuckDB will work with these strings)
    df_prev['geometry_wkt'] = df_prev['geometry'].apply(lambda geom: geom.wkt)
    df_curr['geometry_wkt'] = df_curr['geometry'].apply(lambda geom: geom.wkt)
    
    # Drop the unsupported 'geometry' column (DuckDB cannot register it)
    df_prev = df_prev.drop(columns=['geometry'])
    df_curr = df_curr.drop(columns=['geometry'])
    
    # Split the DataFrames into 4 chunks each
    num_workers = 4 if not num_workers else num_workers
    chunks_prev = np.array_split(df_prev, num_workers)
    chunks_curr = np.array_split(df_curr, num_workers)
    
    # Process each chunk in parallel using multiprocessing Pool
    with mp.Pool(processes=num_workers) as pool:
        results_prev = pool.map(process_h3_info_chunk, chunks_prev)
        results_curr = pool.map(process_h3_info_chunk, chunks_curr)
    
    # Concatenate the results back into single DataFrames
    df_prev_processed = pd.concat(results_prev, ignore_index=True)
    df_curr_processed = pd.concat(results_curr, ignore_index=True)
    
    return df_prev_processed, df_curr_processed
# def create_h3_info(df_prev: pd.DataFrame,
#                    df_curr: pd.DataFrame) -> tuple:
#     """
#     Creates H3 centroids and polyfill using DuckDB UDFs.
    
#     Args:
#         df_prev (pd.DataFrame): Previous DataFrame.
#         df_curr (pd.DataFrame): Current DataFrame.
    
#     Returns:
#         tuple: Updated (df_prev, df_curr) DataFrames with H3 info.
#     """
#     # Create WKT columns
#     df_prev['geometry_wkt'] = df_prev['geometry'].apply(lambda geom: geom.wkt)
#     df_curr['geometry_wkt'] = df_curr['geometry'].apply(lambda geom: geom.wkt)
    
#     # Drop the unsupported 'geometry' column (DuckDB cannot register it)
#     df_prev = df_prev.drop(columns=['geometry'])
#     df_curr = df_curr.drop(columns=['geometry'])
    
#     # Connect to DuckDB and register UDFs
#     con = duckdb.connect()
#     con.create_function('compute_h3_centroid', compute_h3_centroid_udf)
#     con.create_function('compute_h3_polyfill', compute_h3_polyfill_udf)
    
#     # Register the DataFrames as DuckDB tables
#     con.register('df_prev', df_prev)
#     con.register('df_curr', df_curr)
    
#     # Process df_prev
#     df_prev = con.execute('''
#         SELECT
#             *,
#             compute_h3_centroid(geometry_wkt, 6) AS h3_centroid,
#             compute_h3_polyfill(geometry_wkt, 9) AS h3_polyfill
#         FROM df_prev
#     ''').fetch_df()
    
#     # Process df_curr (make sure to query from df_curr)
#     df_curr = con.execute('''
#         SELECT
#             *,
#             compute_h3_centroid(geometry_wkt, 6) AS h3_centroid,
#             compute_h3_polyfill(geometry_wkt, 9) AS h3_polyfill
#         FROM df_curr
#     ''').fetch_df()
    
#     return df_prev, df_curr

# @Timer()  # Time the overall parallel processing
# def create_h3_info_parallel_new(df_prev: pd.DataFrame, df_curr: pd.DataFrame, iceberg_path: str, num_workers: int = 4) -> None:
#     """
#     Computes H3 centroids and polyfill in parallel using 4 workers,
#     and directly saves results as an Apache Iceberg table.
    
#     Args:
#         df_prev (pd.DataFrame): Previous DataFrame.
#         df_curr (pd.DataFrame): Current DataFrame.
#         iceberg_path (str): Path to store the Iceberg table.
#         num_workers (int): Number of parallel workers (default = 4).
#     """
#     import duckdb

#     # Load Iceberg extension in DuckDB
#     con = duckdb.connect()
#     con.sql("INSTALL iceberg;")
#     con.sql("LOAD iceberg;")

#     # Create Iceberg table (if it doesn't exist)
#     con.sql(f"""
#         CREATE TABLE IF NOT EXISTS iceberg.{iceberg_path} (
#             id UUID,
#             geohash STRING,
#             geometry_wkt STRING,
#             h3_centroid STRING,
#             h3_polyfill STRING
#         ) USING iceberg;
#     """)

#     # Create WKT columns (DuckDB will work with these strings)
#     df_prev['geometry_wkt'] = df_prev['geometry'].apply(lambda geom: geom.wkt)
#     df_curr['geometry_wkt'] = df_curr['geometry'].apply(lambda geom: geom.wkt)

#     # Drop the unsupported 'geometry' column (DuckDB cannot register it)
#     df_prev = df_prev.drop(columns=['geometry'])
#     df_curr = df_curr.drop(columns=['geometry'])

#     # Split the DataFrames into 4 chunks each
#     num_workers = 4 if not num_workers else num_workers
#     chunks_prev = np.array_split(df_prev, num_workers)
#     chunks_curr = np.array_split(df_curr, num_workers)

#     # Process each chunk in parallel using multiprocessing Pool
#     with mp.Pool(processes=num_workers) as pool:
#         results_prev = pool.map(process_h3_info_chunk, chunks_prev)
#         results_curr = pool.map(process_h3_info_chunk, chunks_curr)

#     # Concatenate results
#     df_prev_processed = pd.concat(results_prev, ignore_index=True)
#     df_curr_processed = pd.concat(results_curr, ignore_index=True)

#     # Register & Append results to Iceberg Table
#     con.register("df_prev_processed", df_prev_processed)
#     con.register("df_curr_processed", df_curr_processed)
#     con.sql(f"INSERT INTO iceberg.{iceberg_path} SELECT * FROM df_prev_processed;")
#     con.sql(f"INSERT INTO iceberg.{iceberg_path} SELECT * FROM df_curr_processed;")

#     con.close()
#     print(f"Iceberg table updated at {iceberg_path}")

@Timer()
def retrieve_and_save_pg_table(postgresql_details: dict = None, 
                               db_name: str = 'blob_matching', 
                               table_name: str = '', 
                               parquet_path: str = 'filtered_blob.parquet', 
                               log_enabled: bool = True):
    """
    Retrieves filtered data from a PostgreSQL table and saves it as a Parquet file.
    
    Args:
        postgresql_details (dict): Connection details for PostgreSQL.
        db_name (str): Name of the database.
        table_name (str): Name of the table.
        parquet_path (str): Path to save the Parquet file.
        log_enabled (bool): Whether to print logs.
        
    Returns:
        str: Path of the saved Parquet file.
    """

    if postgresql_details is None:
        postgresql_details = pg_details()  # Function that retrieves DB details
    
    if not table_name:
        raise ValueError("Table name must be specified.")

    postgresql_details['dbname'] = db_name
    conn = psycopg2.connect(**postgresql_details)
    cur = conn.cursor()

    # Define SQL query with filtering criteria
    query = sql.SQL(f"""
        SELECT geometry, id, geohash 
        FROM {table_name} 
        WHERE "YEAR" = '2024' 
        AND "MONTH" BETWEEN '03' AND '07';
    """)

    # Retrieve data
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    # Convert to DataFrame
    df = pd.DataFrame(rows, columns=["geometry", "id", "geohash"])
    
    # Convert WKT to Shapely geometry
    df["geometry"] = df["geometry"].apply(lambda x: loads(x) if x else None)
    
    # Remove invalid geometries
    df = df[df["geometry"].notna()]

    # Save to Parquet
    df.to_parquet(parquet_path, engine="pyarrow", compression="snappy")

    if log_enabled:
        print(f"Retrieved {len(df)} records from {table_name} and saved to {parquet_path}")

    return parquet_path  # Return the saved file path

@Timer()
def process_h3_info_chunk(df_chunk: pd.DataFrame) -> pd.DataFrame:
    """
    Processes a single DataFrame chunk using DuckDB UDFs.
    
    Args:
        df_chunk (pd.DataFrame): The chunk of data assigned to the worker.
    
    Returns:
        pd.DataFrame: Processed chunk with H3 centroid and polyfill.
    """
    con = duckdb.connect()
    con.create_function('compute_h3_centroid', compute_h3_centroid_udf)
    con.create_function('compute_h3_polyfill', compute_h3_polyfill_udf)
    con.register('df_chunk', df_chunk)

    result = con.execute('''
        SELECT
            *,
            compute_h3_centroid(geometry_wkt, 6) AS h3_centroid,
            compute_h3_polyfill(geometry_wkt, 9) AS h3_polyfill
        FROM df_chunk
    ''').fetch_df()

    con.close()
    return result

# def create_h3_info_parallel_new(df_prev: pd.DataFrame, df_curr: pd.DataFrame, iceberg_path: str, num_workers: int = 4) -> tuple:
#     """
#     Computes H3 centroids and polyfill in parallel using multiprocessing and saves to an Iceberg table.
    
#     Args:
#         df_prev (pd.DataFrame): Previous DataFrame.
#         df_curr (pd.DataFrame): Current DataFrame.
#         iceberg_path (str): Path to store the Iceberg table.
#         num_workers (int): Number of parallel workers (default = 4).
    
#     Returns:
#         tuple: (df_prev_processed, df_curr_processed)
#     """
#     # Connect to DuckDB
#     con = duckdb.connect()
#     con.sql("INSTALL iceberg;")
#     con.sql("LOAD iceberg;")

#     logging.info("Loaded Iceberg extension in DuckDB")

#     # Create Iceberg table if not exists
#     con.sql(f"""
#         CREATE TABLE IF NOT EXISTS iceberg.{iceberg_path} (
#             id UUID,
#             geohash STRING,
#             geometry_wkt STRING,
#             h3_centroid STRING,
#             h3_polyfill STRING
#         ) USING iceberg;
#     """)
#     logging.info(f"Iceberg table initialized at {iceberg_path}")

#     # Convert geometries to WKT format and drop unsupported column
#     df_prev['geometry_wkt'] = df_prev['geometry'].apply(lambda geom: geom.wkt)
#     df_curr['geometry_wkt'] = df_curr['geometry'].apply(lambda geom: geom.wkt)

#     df_prev = df_prev.drop(columns=['geometry'])
#     df_curr = df_curr.drop(columns=['geometry'])

#     # Total rows
#     total_rows_prev = len(df_prev)
#     total_rows_curr = len(df_curr)

#     logging.info(f"Total rows: df_prev={total_rows_prev}, df_curr={total_rows_curr}")

#     # Split the DataFrames into chunks for workers
#     num_workers = 4 if not num_workers else num_workers
#     chunks_prev = np.array_split(df_prev, num_workers)
#     chunks_curr = np.array_split(df_curr, num_workers)

#     # Log the size of each chunk and total data distribution
#     for i, chunk in enumerate(chunks_prev):
#         logging.info(f"Worker {i} assigned {len(chunk)} rows from df_prev.")
#     for i, chunk in enumerate(chunks_curr):
#         logging.info(f"Worker {i} assigned {len(chunk)} rows from df_curr.")

#     logging.info(f"Starting multiprocessing with {num_workers} workers...")

#     # Process chunks in parallel
#     with mp.Pool(processes=num_workers) as pool:
#         results_prev = pool.map(process_h3_info_chunk, chunks_prev)
#         results_curr = pool.map(process_h3_info_chunk, chunks_curr)

#     logging.info("Parallel H3 computation completed!")

#     # Concatenate processed chunks
#     df_prev_processed = pd.concat(results_prev, ignore_index=True)
#     df_curr_processed = pd.concat(results_curr, ignore_index=True)

#     logging.info(f"Processed df_prev: {len(df_prev_processed)} rows, df_curr: {len(df_curr_processed)} rows.")

#     # Register & append to Iceberg Table
#     con.register("df_prev_processed", df_prev_processed)
#     con.register("df_curr_processed", df_curr_processed)

#     con.sql(f"INSERT INTO iceberg.{iceberg_path} SELECT * FROM df_prev_processed;")
#     con.sql(f"INSERT INTO iceberg.{iceberg_path} SELECT * FROM df_curr_processed;")

#     logging.info(f"Data successfully inserted into Iceberg table at {iceberg_path}")

#     con.close()
#     print(f"Iceberg table updated at {iceberg_path}")
#     logging.info(f"Iceberg table update complete at {iceberg_path}")

#     return df_prev_processed, df_curr_processed
@Timer()  # Time the overall parallel processing
def create_h3_info_parallel_new(df_prev: pd.DataFrame, df_curr: pd.DataFrame, iceberg_path: Path, num_workers: int = 4) -> tuple:
    """
    Computes H3 centroids and polyfill in parallel using multiprocessing and saves to Parquet.
    
    Args:
        df_prev (pd.DataFrame): Previous DataFrame.
        df_curr (pd.DataFrame): Current DataFrame.
        iceberg_path (str): Path to store the Parquet files.
        num_workers (int): Number of parallel workers (default = 4).
    
    Returns:
        tuple: (df_prev_processed, df_curr_processed)
    """
    # processing steps
    if 'geometry' in df_prev.columns:
        df_prev['geometry_wkt'] = df_prev['geometry'].apply(lambda geom: geom.wkt)
    if 'geometry' in df_curr.columns:
        df_curr['geometry_wkt'] = df_curr['geometry'].apply(lambda geom: geom.wkt)

    df_prev = df_prev.drop(columns=['geometry'], errors='ignore')
    df_curr = df_curr.drop(columns=['geometry'], errors='ignore')

    num_workers = 4 if not num_workers else num_workers
    chunks_prev = np.array_split(df_prev, num_workers)
    chunks_curr = np.array_split(df_curr, num_workers)

    with mp.Pool(processes=num_workers) as pool:
        results_prev = pool.map(process_h3_info_chunk, chunks_prev)
        results_curr = pool.map(process_h3_info_chunk, chunks_curr)

    df_prev_processed = pd.concat(results_prev, ignore_index=True)
    df_curr_processed = pd.concat(results_curr, ignore_index=True)

    # Save to Parquet first
    parquet_file_prev = iceberg_path / 'df_prev_processed.parquet'
    # parquet_file_prev = f"{iceberg_path}/df_prev_processed.parquet"
    parquet_file_curr = iceberg_path / 'df_curr_processed.parquet'
    # parquet_file_curr = f"{iceberg_path}/df_curr_processed.parquet"
    df_prev_processed.to_parquet(parquet_file_prev)
    df_curr_processed.to_parquet(parquet_file_curr)

    logging.info(f"Data saved to Parquet: {parquet_file_prev}, {parquet_file_curr}")

    # Load Iceberg catalog
    catalog = load_catalog("local", **{"type": "rest", "uri": "http://localhost:8181"})  # Update with correct Iceberg catalog
    iceberg_table_name = "h3_info_table"

    if iceberg_table_name not in catalog.list_tables():
        schema = Schema([
            ("id", "uuid"),
            ("geohash", "string"),
            ("geometry_wkt", "string"),
            ("h3_centroid", "string"),
            ("h3_polyfill", "string"),
        ])
        catalog.create_table(name=iceberg_table_name, schema=schema, path=iceberg_path)

    table = catalog.load_table(iceberg_table_name)

    # Append Parquet data into Iceberg table
    table.append(parquet_file_prev)
    table.append(parquet_file_curr)

    logging.info(f"Data successfully inserted into Iceberg table at {iceberg_path}")

    return df_prev_processed, df_curr_processed

# @Timer()
# def retrieve_data(chunk_size=100000, use_checkpoint=True, engine_url=None):
#     """
#     Downloads data in chunks from a PostgreSQL database using CTID for pagination.
    
#     - Each chunk contains up to `chunk_size` rows.
#     - After processing each chunk, logs:
#        • the start of the chunk
#        • the chunk number
#        • the distribution of months (counts for each month)
#     - Saves the last CTID and chunk number to a checkpoint file so that processing can resume.
#     - Saves each chunk as a Parquet file named like blob_data_chunk_XX.parquet.
    
#     Parameters:
#       chunk_size (int): Number of rows per chunk (default: 100000)
#       use_checkpoint (bool): If True, resume processing from a saved checkpoint.
#       engine_url (str): Optional SQLAlchemy connection string. If not provided, reads credentials from data files.
#     """
#     # Set up logging (ensure your logging config is set to capture INFO-level messages)
#     logging.basicConfig(level=logging.INFO)
    
#     # Load connection details if engine_url is not provided.
#     if engine_url is None:
#         with open('data/user.txt', 'r') as file:
#             user = file.read().strip()
#         with open('data/pass.txt', 'r') as file:
#             pw = file.read().strip()
#         with open('data/db_host.txt', 'r') as file:
#             host = file.read().strip()
#         with open('data/db_port.txt', 'r') as file:
#             port = file.read().strip()
#         with open('data/db_name.txt', 'r') as file:
#             name = file.read().strip()
    
#         engine_url = f'postgresql://{user}:{pw}@{host}:{port}/{name}'
    
#     engine = create_engine(engine_url)
    
#     # Check for an existing checkpoint
#     checkpoint_path = Path("data/chunk_checkpoint.json")
#     if use_checkpoint and checkpoint_path.exists():
#         with open(checkpoint_path, "r") as f:
#             checkpoint = json.load(f)
#         chunk_number = checkpoint.get("chunk_number", 1)
#         last_ctid = checkpoint.get("last_ctid", None)
#         logging.info(f"Resuming from checkpoint: chunk {chunk_number}, last_ctid = {last_ctid}")
#     else:
#         chunk_number = 1
#         last_ctid = None
    
#     while True:
#         try:
#             # Build the query. Note: ordering by CTID ensures a consistent order.
#             query = f"""
#                 SELECT *, ctid
#                 FROM blob
#                 WHERE "YEAR" = '2024'
#                   AND "MONTH" BETWEEN '03' AND '07'
#             """
#             if last_ctid is not None:
#                 # If last_ctid is an integer (e.g., in our fake dataset) don't quote it.
#                 if isinstance(last_ctid, int):
#                     query += f" AND ctid > {last_ctid}"
#                 else:
#                     query += f" AND ctid > '{last_ctid}'"
#             query += " ORDER BY ctid LIMIT " + str(chunk_size) + ";"
    
#             logging.info(f"Starting chunk {chunk_number}. Last CTID: {last_ctid}")
    
#             # Fetch the chunk into a DataFrame
#             df = pd.read_sql(query, engine)
    
#             if df.empty:
#                 logging.info("No more rows to fetch. Ending pagination.")
#                 # Remove the checkpoint file since we're done
#                 if checkpoint_path.exists():
#                     checkpoint_path.unlink()
#                 break
    
#             # Log month distribution (this logs both which months are present and the row counts)
#             month_counts = df["MONTH"].value_counts().to_dict()
#             logging.info(f"Chunk {chunk_number} month counts: {month_counts}")
    
#             # Save the DataFrame as a Parquet file
#             parquet_path = Path(f"data/blob_data_chunk_{chunk_number}.parquet")
#             df.to_parquet(parquet_path, engine='pyarrow')
#             logging.info(f"Chunk {chunk_number} saved to: {parquet_path}")
    
#             # Get the last CTID from this chunk for pagination
#             last_ctid = df['ctid'].iloc[-1]
#             # Save a checkpoint (chunk number and last CTID) so we can resume later if needed
#             checkpoint_data = {"chunk_number": chunk_number + 1, "last_ctid": last_ctid}
#             with open(checkpoint_path, "w") as f:
#                 json.dump(checkpoint_data, f)
#             # Also, save a separate file for this chunk's checkpoint if desired
#             ct_file_path = Path(f"data/chunk_{chunk_number}_last_ctid_{last_ctid}.txt")
#             with open(ct_file_path, 'w') as f:
#                 f.write(str(last_ctid))
#             logging.info(f"Saved last CTID for chunk {chunk_number} to: {ct_file_path}")
    
#             chunk_number += 1
#         except Exception as e:
#             logging.error(f"Error occurred in chunk {chunk_number}: {e}", exc_info=True)
#             # Optionally, you can write out the checkpoint here as well
#             checkpoint_data = {"chunk_number": chunk_number, "last_ctid": last_ctid}
#             with open(checkpoint_path, "w") as f:
#                 json.dump(checkpoint_data, f)
#             break
# @Timer()
# def retrieve_data(chunk_size=100000, use_checkpoint=True, engine_url=None):
#     """
#     Downloads data in chunks from a PostgreSQL database using CTID for pagination.
    
#     - Each chunk contains up to `chunk_size` rows.
#     - Logs when each chunk starts, the chunk number, and the distribution (counts) of months.
#     - Saves the last CTID and chunk number to a checkpoint file so processing can resume.
#     - Saves each chunk as a Parquet file named like blob_data_chunk_XX.parquet.
    
#     Parameters:
#       chunk_size (int): Number of rows per chunk (default: 100000)
#       use_checkpoint (bool): If True, resume processing from a saved checkpoint.
#       engine_url (str): Optional SQLAlchemy connection string. If not provided, reads credentials from data files.
#     """
#     # Set up logging (make sure your logging config captures INFO-level messages)
#     logging.basicConfig(level=logging.INFO)
    
#     # Load connection details if engine_url is not provided.
#     if engine_url is None:
#         with open('data/user.txt', 'r') as file:
#             user = file.read().strip()
#         with open('data/pass.txt', 'r') as file:
#             pw = file.read().strip()
#         with open('data/db_host.txt', 'r') as file:
#             host = file.read().strip()
#         with open('data/db_port.txt', 'r') as file:
#             port = file.read().strip()
#         with open('data/db_name.txt', 'r') as file:
#             name = file.read().strip()
    
#         engine_url = f'postgresql://{user}:{pw}@{host}:{port}/{name}'
    
#     engine = create_engine(engine_url)
    
#     # Check for an existing checkpoint.
#     checkpoint_path = Path("data/chunk_checkpoint.json")
#     if use_checkpoint and checkpoint_path.exists():
#         with open(checkpoint_path, "r") as f:
#             checkpoint = json.load(f)
#         chunk_number = checkpoint.get("chunk_number", 1)
#         last_ctid = checkpoint.get("last_ctid", None)
#         logging.info(f"Resuming from checkpoint: chunk {chunk_number}, last_ctid = {last_ctid}")
#     else:
#         chunk_number = 1
#         last_ctid = None
    
#     while True:
#         try:
#             # Build the query. We alias the system CTID as pgt_ctid to avoid duplicates.
#             query = f"""
#                 SELECT *, ctid as pgt_ctid
#                 FROM blob
#                 WHERE "YEAR" = '2024'
#                   AND "MONTH" BETWEEN '03' AND '07'
#             """
#             if last_ctid is not None:
#                 # If last_ctid is numeric (e.g. in our fake dataset) don't quote it.
#                 if isinstance(last_ctid, int):
#                     query += f" AND pgt_ctid > {last_ctid}"
#                 else:
#                     query += f" AND pgt_ctid > '{last_ctid}'"
#             query += " ORDER BY pgt_ctid LIMIT " + str(chunk_size) + ";"
    
#             logging.info(f"Starting chunk {chunk_number}. Last CTID: {last_ctid}")
    
#             # Fetch the chunk into a DataFrame.
#             df = pd.read_sql(query, engine)
    
#             if df.empty:
#                 logging.info("No more rows to fetch. Ending pagination.")
#                 # Remove the checkpoint file since we're done.
#                 if checkpoint_path.exists():
#                     checkpoint_path.unlink()
#                 break
    
#             # Log month distribution (keys are months; values are counts).
#             month_counts = df["MONTH"].value_counts().to_dict()
#             logging.info(f"Chunk {chunk_number} month counts: {month_counts}")
    
#             # Save the DataFrame as a Parquet file.
#             parquet_path = Path(f"data/blob_data_chunk_{chunk_number}.parquet")
#             df.to_parquet(parquet_path, engine='pyarrow')
#             logging.info(f"Chunk {chunk_number} saved to: {parquet_path}")
    
#             # Get the last CTID from this chunk for pagination.
#             last_ctid = df['pgt_ctid'].iloc[-1]
#             # Convert last_ctid to a native Python int if it's a numpy.int64
#             last_ctid = int(last_ctid)
    
#             # Save a checkpoint so that we can resume processing later.
#             checkpoint_data = {"chunk_number": chunk_number + 1, "last_ctid": last_ctid}
#             with open(checkpoint_path, "w") as f:
#                 json.dump(checkpoint_data, f)
#             # Also, save a separate file for this chunk's checkpoint if desired.
#             ct_file_path = Path(f"data/chunk_{chunk_number}_last_ctid_{last_ctid}.txt")
#             with open(ct_file_path, 'w') as f:
#                 f.write(str(last_ctid))
#             logging.info(f"Saved last CTID for chunk {chunk_number} to: {ct_file_path}")
    
#             chunk_number += 1
#         except Exception as e:
#             logging.error(f"Error occurred in chunk {chunk_number}: {e}", exc_info=True)
#             # Save a checkpoint upon error so that resumption is possible.
#             checkpoint_data = {"chunk_number": chunk_number, "last_ctid": int(last_ctid) if last_ctid is not None else None}
#             with open(checkpoint_path, "w") as f:
#                 json.dump(checkpoint_data, f)
#             break
# @Timer()
# def retrieve_data(chunk_size=100000, use_checkpoint=True, engine_url=None, max_chunks=None):
#     """
#     Downloads data in chunks from a PostgreSQL database using CTID for pagination.
    
#     - Each chunk contains up to `chunk_size` rows.
#     - Logs when each chunk starts, the chunk number, and the distribution (counts) of months.
#     - Saves the last CTID and chunk number to a checkpoint file so processing can resume.
#     - Saves each chunk as a Parquet file named like blob_data_chunk_XX.parquet.
    
#     Parameters:
#       chunk_size (int): Number of rows per chunk (default: 100000)
#       use_checkpoint (bool): If True, resume processing from a saved checkpoint.
#       engine_url (str): Optional SQLAlchemy connection string. If not provided, reads credentials from data files.
#       max_chunks (int): Optional; maximum number of chunks to process. If None, process the entire dataset.
#     """
#     logging.basicConfig(level=logging.INFO)
    
#     # Load connection details if engine_url is not provided.
#     if engine_url is None:
#         with open('data/user.txt', 'r') as file:
#             user = file.read().strip()
#         with open('data/pass.txt', 'r') as file:
#             pw = file.read().strip()
#         with open('data/db_host.txt', 'r') as file:
#             host = file.read().strip()
#         with open('data/db_port.txt', 'r') as file:
#             port = file.read().strip()
#         with open('data/db_name.txt', 'r') as file:
#             name = file.read().strip()
#         engine_url = f'postgresql://{user}:{pw}@{host}:{port}/{name}'
    
#     engine = create_engine(engine_url)
    
#     checkpoint_path = Path("data/chunk_checkpoint.json")
#     if use_checkpoint and checkpoint_path.exists():
#         with open(checkpoint_path, "r") as f:
#             checkpoint = json.load(f)
#         chunk_number = checkpoint.get("chunk_number", 1)
#         last_ctid = checkpoint.get("last_ctid", None)
#         logging.info(f"Resuming from checkpoint: chunk {chunk_number}, last_ctid = {last_ctid}")
#     else:
#         chunk_number = 1
#         last_ctid = None
    
#     while True:
#         # If a stopping condition is provided, check it.
#         if max_chunks is not None and chunk_number > max_chunks:
#             logging.info(f"Reached maximum chunk limit ({max_chunks}). Stopping pagination.")
#             break
        
#         try:
#             # Build query with CTID aliased as pgt_ctid.
#             query = f"""
#                 SELECT *, ctid as pgt_ctid
#                 FROM blob
#                 WHERE "YEAR" = '2024'
#                   AND "MONTH" BETWEEN '03' AND '07'
#             """
#             if last_ctid is not None:
#                 if isinstance(last_ctid, int):
#                     query += f" AND pgt_ctid > {last_ctid}"
#                 else:
#                     query += f" AND pgt_ctid > '{last_ctid}'"
#             query += " ORDER BY pgt_ctid LIMIT " + str(chunk_size) + ";"
    
#             logging.info(f"Starting chunk {chunk_number}. Last CTID: {last_ctid}")
    
#             df = pd.read_sql(query, engine)
#             if df.empty:
#                 logging.info("No more rows to fetch. Ending pagination.")
#                 if checkpoint_path.exists():
#                     checkpoint_path.unlink()
#                 break
    
#             month_counts = df["MONTH"].value_counts().to_dict()
#             logging.info(f"Chunk {chunk_number} month counts: {month_counts}")
    
#             parquet_path = Path(f"data/blob_data_chunk_{chunk_number}.parquet")
#             df.to_parquet(parquet_path, engine='pyarrow')
#             logging.info(f"Chunk {chunk_number} saved to: {parquet_path}")
    
#             last_ctid = int(df['pgt_ctid'].iloc[-1])
#             checkpoint_data = {"chunk_number": chunk_number + 1, "last_ctid": last_ctid}
#             with open(checkpoint_path, "w") as f:
#                 json.dump(checkpoint_data, f)
    
#             ct_file_path = Path(f"data/chunk_{chunk_number}_last_ctid_{last_ctid}.txt")
#             with open(ct_file_path, 'w') as f:
#                 f.write(str(last_ctid))
#             logging.info(f"Saved last CTID for chunk {chunk_number} to: {ct_file_path}")
    
#             chunk_number += 1
#         except Exception as e:
#             logging.error(f"Error occurred in chunk {chunk_number}: {e}", exc_info=True)
#             checkpoint_data = {"chunk_number": chunk_number, "last_ctid": int(last_ctid) if last_ctid is not None else None}
#             with open(checkpoint_path, "w") as f:
#                 json.dump(checkpoint_data, f)
#             break
# @Timer()
# def retrieve_data(chunk_size=100000, use_checkpoint=True, engine_url=None, max_chunks=None, parquet_name='blob_data'):
#     """
#     Downloads data in chunks from a PostgreSQL or SQLite database using CTID for pagination.
    
#     - Each chunk contains up to `chunk_size` rows.
#     - Logs when each chunk starts, the chunk number, and the distribution (counts) of months.
#     - Saves the last CTID and chunk number to a checkpoint file so processing can resume.
#     - Saves each chunk as a Parquet file named like blob_data_chunk_XX.parquet.
    
#     Parameters:
#       chunk_size (int): Number of rows per chunk (default: 100000)
#       use_checkpoint (bool): If True, resume processing from a saved checkpoint.
#       engine_url (str): Optional SQLAlchemy connection string. If not provided, reads credentials from data files.
#       max_chunks (int): Optional; maximum number of chunks to process. If None, process the entire dataset.
#       parquet_name (str): Base name for the output parquet files (default: 'blob_data').
#     """
#     logging.basicConfig(level=logging.INFO)
    
#     # Load connection details if engine_url is not provided.
#     if engine_url is None:
#         with open('data/user.txt', 'r') as file:
#             user = file.read().strip()
#         with open('data/pass.txt', 'r') as file:
#             pw = file.read().strip()
#         with open('data/db_host.txt', 'r') as file:
#             host = file.read().strip()
#         with open('data/db_port.txt', 'r') as file:
#             port = file.read().strip()
#         with open('data/db_name.txt', 'r') as file:
#             name = file.read().strip()
#         engine_url = f'postgresql://{user}:{pw}@{host}:{port}/{name}'
    
#     engine = create_engine(engine_url)
    
#     # Determine if we're working with SQLite.
#     is_sqlite = engine_url.lower().startswith("sqlite://")
    
#     checkpoint_path = Path("data/chunk_checkpoint.json")
#     if use_checkpoint and checkpoint_path.exists():
#         with open(checkpoint_path, "r") as f:
#             checkpoint = json.load(f)
#         chunk_number = checkpoint.get("chunk_number", 1)
#         last_ctid = checkpoint.get("last_ctid", None)
#         logging.info(f"Resuming from checkpoint: chunk {chunk_number}, last_ctid = {last_ctid}")
#     else:
#         chunk_number = 1
#         last_ctid = None
    
#     while True:
#         # Check the stopping condition.
#         if max_chunks is not None and chunk_number > max_chunks:
#             logging.info(f"Reached maximum chunk limit ({max_chunks}). Stopping pagination.")
#             break
        
#         try:
#             # Build the query. 
#             # For PostgreSQL, cast CTID to text; for SQLite, use the existing (numeric) ctid.
#             if is_sqlite:
#                 query = f"""
#                     SELECT *, ctid as pgt_ctid
#                     FROM blob
#                     WHERE "YEAR" = '2024'
#                       AND "MONTH" BETWEEN '03' AND '07'
#                 """
#             else:
#                 query = f"""
#                     SELECT *, CAST(ctid AS text) as pgt_ctid
#                     FROM blob
#                     WHERE "YEAR" = '2024'
#                       AND "MONTH" BETWEEN '03' AND '07'
#                 """
#             if last_ctid is not None:
#                 if isinstance(last_ctid, int):
#                     query += f" AND pgt_ctid > {last_ctid}"
#                 else:
#                     query += f" AND pgt_ctid > '{last_ctid}'"
#             query += " ORDER BY pgt_ctid LIMIT " + str(chunk_size) + ";"
    
#             logging.info(f"Starting chunk {chunk_number}. Last CTID: {last_ctid}")
    
#             df = pd.read_sql(query, engine)
#             if df.empty:
#                 logging.info("No more rows to fetch. Ending pagination.")
#                 # Optionally, comment out the unlink below if you want to keep the checkpoint.
#                 if checkpoint_path.exists():
#                     checkpoint_path.unlink()
#                 break
    
#             month_counts = df["MONTH"].value_counts().to_dict()
#             logging.info(f"Chunk {chunk_number} month counts: {month_counts}")
    

#             parquet_path = Path(f"data/{parquet_name}_chunk_{chunk_number}.parquet")
#             df.to_parquet(parquet_path, engine='pyarrow')
#             logging.info(f"Chunk {chunk_number} saved to: {parquet_path}")
    
#             # When working with SQLite, our fake CTID is an integer;
#             # with PostgreSQL, pgt_ctid is a string. We store it as-is.
#             if is_sqlite:
#                 last_ctid = int(df['pgt_ctid'].iloc[-1])
#             else:
#                 last_ctid = df['pgt_ctid'].iloc[-1]
    
#             checkpoint_data = {"chunk_number": chunk_number + 1, "last_ctid": last_ctid}
#             with open(checkpoint_path, "w") as f:
#                 json.dump(checkpoint_data, f)
    
#             ct_file_path = Path(f"data/chunk_{chunk_number}_last_ctid_{last_ctid}.txt")
#             with open(ct_file_path, 'w') as f:
#                 f.write(str(last_ctid))
#             logging.info(f"Saved last CTID for chunk {chunk_number} to: {ct_file_path}")
    
#             chunk_number += 1
#         except Exception as e:
#             logging.error(f"Error occurred in chunk {chunk_number}: {e}", exc_info=True)
#             checkpoint_data = {"chunk_number": chunk_number, "last_ctid": int(last_ctid) if last_ctid is not None and is_sqlite else last_ctid}
#             with open(checkpoint_path, "w") as f:
#                 json.dump(checkpoint_data, f)
#             break
# @Timer()
# def retrieve_data(chunk_size=100000, use_checkpoint=True, engine_url=None, max_chunks=None, prefix='blob_data'):
#     """
#     Downloads data in chunks from a PostgreSQL or SQLite database using CTID for pagination.
    
#     - Each chunk contains up to `chunk_size` rows.
#     - Logs when each chunk starts, the chunk number, and the distribution (counts) of months.
#     - Saves the last CTID and chunk number to a checkpoint file so processing can resume.
#     - Saves each chunk as a Parquet file named like <prefix>_chunk_XX.parquet.
    
#     Parameters:
#       chunk_size (int): Number of rows per chunk (default: 100000)
#       use_checkpoint (bool): If True, resume processing from a saved checkpoint.
#       engine_url (str): Optional SQLAlchemy connection string. If not provided, reads credentials from data files.
#       max_chunks (int): Optional; maximum number of chunks to process. If None, process the entire dataset.
#       prefix (str): Base name for the output parquet and txt files (default: 'blob_data').
#     """
#     logging.basicConfig(level=logging.INFO)
    
#     # Load connection details if engine_url is not provided.
#     if engine_url is None:
#         with open('data/user.txt', 'r') as file:
#             user = file.read().strip()
#         with open('data/pass.txt', 'r') as file:
#             pw = file.read().strip()
#         with open('data/db_host.txt', 'r') as file:
#             host = file.read().strip()
#         with open('data/db_port.txt', 'r') as file:
#             port = file.read().strip()
#         with open('data/db_name.txt', 'r') as file:
#             name = file.read().strip()
#         engine_url = f'postgresql://{user}:{pw}@{host}:{port}/{name}'
    
#     engine = create_engine(engine_url)
    
#     # Determine if we're working with SQLite.
#     is_sqlite = engine_url.lower().startswith("sqlite://")
    
#     checkpoint_path = Path("data/chunk_checkpoint.json")
#     if use_checkpoint and checkpoint_path.exists():
#         with open(checkpoint_path, "r") as f:
#             checkpoint = json.load(f)
#         chunk_number = checkpoint.get("chunk_number", 1)
#         last_ctid = checkpoint.get("last_ctid", None)
#         logging.info(f"Resuming from checkpoint: chunk {chunk_number}, last_ctid = {last_ctid}")
#     else:
#         chunk_number = 1
#         last_ctid = None
    
#     while True:
#         # Check the stopping condition.
#         if max_chunks is not None and chunk_number > max_chunks:
#             logging.info(f"Reached maximum chunk limit ({max_chunks}). Stopping pagination.")
#             break
        
#         try:
#             # Build the query.
#             if is_sqlite:
#                 # For SQLite, we assume a numeric ctid.
#                 query = f"""
#                     SELECT *, ctid as pgt_ctid
#                     FROM blob
#                     WHERE "YEAR" = '2024'
#                       AND "MONTH" BETWEEN '03' AND '07'
#                 """
#                 if last_ctid is not None:
#                     query += f" AND pgt_ctid > {last_ctid}"
#                 query += " ORDER BY pgt_ctid LIMIT " + str(chunk_size) + ";"
#             else:
#                 # For PostgreSQL, use a subquery so that we can filter on the alias.
#                 query = f"""
#                     SELECT *
#                     FROM (
#                         SELECT *, CAST(ctid AS text) as pgt_ctid
#                         FROM blob
#                         WHERE "YEAR" = '2024'
#                           AND "MONTH" BETWEEN '03' AND '07'
#                     ) sub
#                 """
#                 if last_ctid is not None:
#                     query += f" WHERE pgt_ctid > '{last_ctid}'"
#                 query += " ORDER BY pgt_ctid LIMIT " + str(chunk_size) + ";"
    
#             logging.info(f"Starting chunk {chunk_number}. Last CTID: {last_ctid}")
    
#             df = pd.read_sql(query, engine)
#             if df.empty:
#                 logging.info("No more rows to fetch. Ending pagination.")
#                 # Optionally, comment out the next line if you want to keep the checkpoint file.
#                 if checkpoint_path.exists():
#                     checkpoint_path.unlink()
#                 break
    
#             month_counts = df["MONTH"].value_counts().to_dict()
#             logging.info(f"Chunk {chunk_number} month counts: {month_counts}")
    
#             parquet_path = Path(f"data/{prefix}_chunk_{chunk_number}.parquet")
#             df.to_parquet(parquet_path, engine='pyarrow')
#             logging.info(f"Chunk {chunk_number} saved to: {parquet_path}")
    
#             # Set last_ctid for the next iteration.
#             if is_sqlite:
#                 last_ctid = int(df['pgt_ctid'].iloc[-1])
#             else:
#                 last_ctid = df['pgt_ctid'].iloc[-1]
    
#             checkpoint_data = {"chunk_number": chunk_number + 1, "last_ctid": last_ctid}
#             with open(checkpoint_path, "w") as f:
#                 json.dump(checkpoint_data, f)
    
#             ct_file_path = Path(f"data/{prefix}_chunk_{chunk_number}_last_ctid_{last_ctid}.txt")
#             with open(ct_file_path, 'w') as f:
#                 f.write(str(last_ctid))
#             logging.info(f"Saved last CTID for chunk {chunk_number} to: {ct_file_path}")
    
#             chunk_number += 1
#         except Exception as e:
#             logging.error(f"Error occurred in chunk {chunk_number}: {e}", exc_info=True)
#             checkpoint_data = {"chunk_number": chunk_number, "last_ctid": int(last_ctid) if (last_ctid is not None and is_sqlite) else last_ctid}
#             with open(checkpoint_path, "w") as f:
#                 json.dump(checkpoint_data, f)
#             break
@Timer()
def retrieve_data(chunk_size=100000, use_checkpoint=True, engine_url=None, max_chunks=None, prefix='blob_data'):
    """
    Downloads data in chunks from a PostgreSQL or SQLite database using CTID for pagination.
    
    - Each chunk contains up to `chunk_size` rows.
    - Logs when each chunk starts, the chunk number, and the distribution (counts) of months.
    - Saves the last CTID and chunk number to a checkpoint file so processing can resume.
    - Saves each chunk as a Parquet file named like <prefix>_chunk_XX.parquet.
    
    Parameters:
      chunk_size (int): Number of rows per chunk (default: 100000)
      use_checkpoint (bool): If True, resume processing from a saved checkpoint.
      engine_url (str): Optional SQLAlchemy connection string. If not provided, reads credentials from data files.
      max_chunks (int): Optional; maximum number of chunks to process. If None, process the entire dataset.
      prefix (str): Base name for the output parquet and txt files (default: 'blob_data').
    """
    logging.basicConfig(level=logging.INFO)
    
    # Load connection details if engine_url is not provided.
    if engine_url is None:
        with open('data/user.txt', 'r') as file:
            user = file.read().strip()
        with open('data/pass.txt', 'r') as file:
            pw = file.read().strip()
        with open('data/db_host.txt', 'r') as file:
            host = file.read().strip()
        with open('data/db_port.txt', 'r') as file:
            port = file.read().strip()
        with open('data/db_name.txt', 'r') as file:
            name = file.read().strip()
        engine_url = f'postgresql://{user}:{pw}@{host}:{port}/{name}'
    
    engine = create_engine(engine_url)
    
    # Determine if we're working with SQLite.
    is_sqlite = engine_url.lower().startswith("sqlite://")
    
    # (Optional) If you want separate checkpoint files for different databases,
    # you could incorporate the prefix into the checkpoint filename:
    checkpoint_path = Path(f"data/{prefix}_chunk_checkpoint.json")
    
    if use_checkpoint and checkpoint_path.exists():
        with open(checkpoint_path, "r") as f:
            checkpoint = json.load(f)
        chunk_number = checkpoint.get("chunk_number", 1)
        last_ctid = checkpoint.get("last_ctid", None)
        logging.info(f"Resuming from checkpoint: chunk {chunk_number}, last_ctid = {last_ctid}")
    else:
        chunk_number = 1
        last_ctid = None
    
    while True:
        # Check the stopping condition.
        if max_chunks is not None and chunk_number > max_chunks:
            logging.info(f"Reached maximum chunk limit ({max_chunks}). Stopping pagination.")
            break
        
        try:
            # Build the query.
            if is_sqlite:
                query = f"""
                    SELECT *, ctid as pgt_ctid
                    FROM blob
                    WHERE "YEAR" = '2024'
                      AND "MONTH" BETWEEN '03' AND '07'
                """
                if last_ctid is not None:
                    query += f" AND pgt_ctid > {last_ctid}"
                query += " ORDER BY pgt_ctid LIMIT " + str(chunk_size) + ";"
            else:
                query = f"""
                    SELECT *
                    FROM (
                        SELECT *, CAST(ctid AS text) as pgt_ctid
                        FROM blob
                        WHERE "YEAR" = '2024'
                          AND "MONTH" BETWEEN '03' AND '07'
                    ) sub
                """
                if last_ctid is not None:
                    query += f" WHERE pgt_ctid > '{last_ctid}'"
                query += " ORDER BY pgt_ctid LIMIT " + str(chunk_size) + ";"
    
            logging.info(f"Starting chunk {chunk_number}. Last CTID: {last_ctid}")
            print(f"Starting chunk {chunk_number}. Last CTID: {last_ctid}")
    
            df = pd.read_sql(query, engine)
            if df.empty:
                logging.info("No more rows to fetch. Ending pagination.")
                if checkpoint_path.exists():
                    checkpoint_path.unlink()
                break
    
            month_counts = df["MONTH"].value_counts().to_dict()
            logging.info(f"Chunk {chunk_number} month counts: {month_counts}")
    
            parquet_path = Path(f"data/{prefix}_chunk_{chunk_number}.parquet")
            df.to_parquet(parquet_path, engine='pyarrow')
            logging.info(f"Chunk {chunk_number} saved to: {parquet_path}")
    
            if is_sqlite:
                last_ctid = int(df['pgt_ctid'].iloc[-1])
            else:
                last_ctid = df['pgt_ctid'].iloc[-1]
    
            checkpoint_data = {"chunk_number": chunk_number + 1, "last_ctid": last_ctid}
            with open(checkpoint_path, "w") as f:
                json.dump(checkpoint_data, f)
    
            ct_file_path = Path(f"data/{prefix}_chunk_{chunk_number}_last_ctid_{last_ctid}.txt")
            with open(ct_file_path, 'w') as f:
                f.write(str(last_ctid))
            logging.info(f"Saved last CTID for chunk {chunk_number} to: {ct_file_path}")
    
            chunk_number += 1
        except Exception as e:
            logging.error(f"Error occurred in chunk {chunk_number}: {e}", exc_info=True)
            checkpoint_data = {"chunk_number": chunk_number, "last_ctid": int(last_ctid) if (last_ctid is not None and is_sqlite) else last_ctid}
            with open(checkpoint_path, "w") as f:
                json.dump(checkpoint_data, f)
            break

# For confirmation:
# The following code logs the month distribution for the current chunk.
# It uses:
#     month_counts = df["MONTH"].value_counts().to_dict()
# which produces a dictionary where keys are the month values (e.g., '03', '04', etc.)
# and values are the counts of rows for each month in that chunk.
# This meets your requirement to log which months are present and how many rows each month has.

