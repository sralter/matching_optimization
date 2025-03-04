import time, math, itertools, logging, random
import multiprocessing as mp
from multiprocessing import Pool, Manager
import psycopg2
import psycopg2.extras as extras
from psycopg2 import sql
import geopandas as gpd
import pandas as pd
import shapely.geometry
from shapely.wkt import loads
import psutil

# ================================
# Database and Retrieval Functions
# ================================

def create_matched_results_table(postgresql_details: dict, db_name: str, table_name: str):
    """
    Drops the matched_results table if it exists and creates a new one with a unique constraint.
    """
    postgresql_details['dbname'] = db_name
    conn = psycopg2.connect(**postgresql_details)
    cur = conn.cursor()
    create_table_query = sql.SQL("""
        DROP TABLE IF EXISTS {};
        CREATE TABLE {} (
            prev_id UUID,
            curr_id UUID,
            UNIQUE(prev_id, curr_id)
        );
    """).format(sql.Identifier(table_name), sql.Identifier(table_name))
    cur.execute(create_table_query)
    conn.commit()
    cur.close()
    conn.close()
    print(f"Table {table_name} created successfully.")

def _retrieve_pg_table(postgresql_details: dict = None, db_name: str = 'blob_matching', table_name: str = '', log_queue=None):
    """
    Retrieves data from a PostgreSQL table and converts WKT back to geometries.
    """
    if postgresql_details is None:
        raise ValueError("PostgreSQL details must be provided.")
    if not table_name:
        raise ValueError("Table name must be specified.")
    postgresql_details['dbname'] = db_name
    conn = psycopg2.connect(**postgresql_details)
    cur = conn.cursor()
    cur.execute(sql.SQL(f"SELECT geometry, id, geohash FROM {table_name};"))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    df = pd.DataFrame(rows, columns=["geometry", "id", "geohash"])
    df["geometry"] = df["geometry"].apply(loads)  # Convert WKT to Shapely geometry
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

def worker_logger(start_time, memory_start, log_queue):
    """
    Logs final CPU, memory, and execution time at the end of processing.
    """
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

def logging_listener(log_queue):
    """
    Listener function that handles logs coming from multiple processes.
    """
    while True:
        try:
            record = log_queue.get()
            if record is None:
                break  # Sentinel value to exit
            logger = logging.getLogger(record.name)
            logger.handle(record)
        except EOFError:
            print("Log queue closed unexpectedly.")
            break
        except Exception as e:
            print(f"Logging error: {e}")
    print('Logging listener has shut down.')

# ================================
# Spatial Grid Partitioning Functions
# ================================

def create_spatial_grid(gdf, num_workers):
    """
    Create a grid (vertical slices) covering the entire extent of gdf.
    Returns a GeoDataFrame of grid cells with a 'cell_id' column.
    """
    xmin, ymin, xmax, ymax = gdf.total_bounds
    cell_width = (xmax - xmin) / num_workers
    cells = []
    for i in range(num_workers):
        cell = shapely.geometry.box(xmin + i * cell_width, ymin, xmin + (i + 1) * cell_width, ymax)
        cells.append(cell)
    grid = gpd.GeoDataFrame({'geometry': cells}, crs=grid.crs)
    grid['cell_id'] = range(num_workers)
    return grid

def assign_polygons_to_grid(gdf, grid):
    """
    Assign each polygon to a grid cell based on its centroid.
    Returns a GeoDataFrame with a new column 'cell_id'.
    """
    gdf = gdf.copy()
    gdf['centroid'] = gdf.geometry.centroid
    joined = gpd.sjoin(gdf, grid, how='left', predicate='within')
    return joined

def create_spatial_partitions(gdf_prev, gdf_curr, num_workers):
    """
    Partition the previous and current GeoDataFrames into non-overlapping spatial cells.
    Returns a list of tuples: (subset_prev, subset_curr, cell_id).
    """
    grid = create_spatial_grid(gdf_prev, num_workers)
    gdf_prev_part = assign_polygons_to_grid(gdf_prev, grid)
    gdf_curr_part = assign_polygons_to_grid(gdf_curr, grid)
    partitions = []
    for cell_id in grid['cell_id']:
        subset_prev = gdf_prev_part[gdf_prev_part['cell_id'] == cell_id]
        subset_curr = gdf_curr_part[gdf_curr_part['cell_id'] == cell_id]
        partitions.append((subset_prev, subset_curr, cell_id))
    return partitions

# ================================
# Worker Function for Each Partition
# ================================

def process_partition(gdf_prev, gdf_curr, output_table, postgresql_details, db_name, log_queue, match_count, lock, cell_id):
    """
    Process a spatial partition (grid cell) by performing a spatial join on its subset.
    """
    start_time = time.time()
    memory_start = psutil.virtual_memory().used / (1024 ** 2)

    log_queue.put(logging.LogRecord(
        name="multiprocessing_logger",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg=f"Processing cell {cell_id}: {len(gdf_prev)} prev, {len(gdf_curr)} curr polygons",
        args=None,
        exc_info=None
    ))
    
    # Rename id columns for clarity
    gdf_prev = gdf_prev.copy().rename(columns={"id": "prev_id"})
    gdf_curr = gdf_curr.copy().rename(columns={"id": "curr_id"})
    
    # Drop any reserved columns that might have been added in previous spatial joins.
    gdf_prev = gdf_prev.drop(columns=["index_left", "index_right"], errors="ignore")
    gdf_curr = gdf_curr.drop(columns=["index_left", "index_right"], errors="ignore")
    
    # Perform spatial join with predicate "intersects"
    joined = gpd.sjoin(gdf_prev, gdf_curr, how="inner", predicate="intersects")
    matches = list(zip(joined["prev_id"], joined["curr_id"]))
    
    with lock:
        match_count.value += len(matches)
    
    log_queue.put(logging.LogRecord(
        name="multiprocessing_logger",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg=f"Cell {cell_id} found {len(matches)} matches",
        args=None,
        exc_info=None
    ))
    
    # Store matches in the database
    if matches:
        conn = psycopg2.connect(**postgresql_details)
        cur = conn.cursor()
        insert_query = sql.SQL(f"INSERT INTO {output_table} (prev_id, curr_id) VALUES %s")
        try:
            extras.execute_values(cur, insert_query, matches)
            conn.commit()
        except Exception as e:
            log_queue.put(logging.LogRecord(
                name="multiprocessing_logger",
                level=logging.ERROR,
                pathname="",
                lineno=0,
                msg=f"Cell {cell_id} - Database insert failed: {e}",
                args=None,
                exc_info=True
            ))
            conn.rollback()
        finally:
            cur.close()
            conn.close()
    
    elapsed_time = time.time() - start_time
    memory_end = psutil.virtual_memory().used / (1024 ** 2)
    memory_used = abs(memory_end - memory_start)
    log_queue.put(logging.LogRecord(
        name="multiprocessing_logger",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg=f"Cell {cell_id} completed in {elapsed_time:.4f} sec, Memory Used: {memory_used:.2f} MB",
        args=None,
        exc_info=None
    ))

# ================================
# Main Parallel Matching Function Using Spatial Grid Partitions
# ================================

def run_parallel_matching_with_grid(table_prev, table_curr, output_table, postgresql_details, db_name, num_workers=4, verbose=2):
    """
    Runs parallel matching using spatial grid partitions.
    Each worker processes a distinct spatial cell.
    """
    manager = Manager()
    log_queue = manager.Queue()
    match_count = manager.Value('i', 0)
    lock = manager.Lock()

    log_process = mp.Process(target=logging_listener, args=(log_queue,))
    log_process.start()

    create_matched_results_table(postgresql_details, db_name, output_table)

    # Retrieve full datasets
    df_prev = _retrieve_pg_table(postgresql_details, db_name, table_prev, log_queue)
    df_curr = _retrieve_pg_table(postgresql_details, db_name, table_curr, log_queue)
    
    # Convert to GeoDataFrames
    gdf_prev = gpd.GeoDataFrame(df_prev, geometry='geometry', crs="EPSG:4326")
    gdf_curr = gpd.GeoDataFrame(df_curr, geometry='geometry', crs="EPSG:4326")

    # Create spatial partitions
    partitions = create_spatial_partitions(gdf_prev, gdf_curr, num_workers)
    print(f"Total partitions to process: {len(partitions)}")

    # Process each partition in parallel
    with Pool(processes=num_workers) as pool:
        pool.starmap(process_partition, [
            (subset_prev, subset_curr, output_table, postgresql_details, db_name, log_queue, match_count, lock, cell_id)
            for subset_prev, subset_curr, cell_id in partitions
        ])

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

# ================================
# Example Main Block
# ================================

if __name__ == '__main__':
    # Define your PostgreSQL connection details here.
    postgresql_details = {
        'user': 'youruser',
        'password': 'yourpassword',
        'host': 'localhost',
        'port': 5432,
        'dbname': 'blob_matching'
    }
    
    run_parallel_matching_with_grid(
        table_prev='prev_blobs_wkt',
        table_curr='curr_blobs_wkt',
        output_table='matched_results',
        postgresql_details=postgresql_details,
        db_name='blob_matching',
        num_workers=5,
        verbose=2
    )
