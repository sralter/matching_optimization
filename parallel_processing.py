import helpers as h
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

@h.Timer()

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

def match_geometries_sjoin(gdf_prev, gdf_curr, log_queue, match_count, lock):
    """
    Match geometries using a spatial join.
    Returns a list of (prev_id, curr_id) tuples for intersecting polygons.
    
    Both gdf_prev and gdf_curr should be GeoDataFrames that include an 'id' column.
    """
    log_queue.put(logging.LogRecord(
        name="multiprocessing_logger",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="Starting match_geometries_sjoin()",
        args=None,
        exc_info=None
    ))
    
    # Ensure both DataFrames have an index column or a unique identifier.
    # You might want to rename the id columns for clarity before the join.
    # For example, add suffixes to differentiate:
    gdf_prev = gdf_prev.copy().rename(columns={"id": "prev_id"})
    gdf_curr = gdf_curr.copy().rename(columns={"id": "curr_id"})
    
    # Perform spatial join with predicate "intersects"
    joined = gpd.sjoin(gdf_prev, gdf_curr, how="inner", predicate="intersects")
    
    # The result will include columns from both GeoDataFrames.
    # By default, GeoPandas will add a suffix to overlapping column names.
    matches = list(zip(joined["prev_id"], joined["curr_id"]))
    
    with lock:
        match_count.value += len(matches)
    
    log_queue.put(logging.LogRecord(
        name="multiprocessing_logger",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg=f"match_geometries_sjoin found {len(matches)} matches",
        args=None,
        exc_info=None
    ))
    
    return matches

def get_neighbors(ghash, precision=1):
    """Compute the 8 neighboring geohashes."""
    lat, lon = geohash.decode(ghash)  # Decode geohash into latitude & longitude
    lat, lon = float(lat), float(lon)  # 🔧 Ensure values are floats

    delta = 10 ** (-precision) # precision = 7  # Small shift based on precision

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
    # df_prev = df_prev[df_prev['geohash'].isin(neighboring_geohashes)]
    # df_curr = df_curr[df_curr['geohash'].isin(neighboring_geohashes)]

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

    # Convert to GeoDataFrames if they aren't already
    gdf_prev = gpd.GeoDataFrame(df_prev, geometry='geometry', crs="EPSG:4326")
    gdf_curr = gpd.GeoDataFrame(df_curr, geometry='geometry', crs="EPSG:4326")

    try:
        matched_pairs = match_geometries_sjoin(gdf_prev, gdf_curr, log_queue, match_count, lock)
        # matched_pairs = match_geometries(df_prev, df_curr, log_queue, match_count, lock)
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
        matched_pairs = []

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
