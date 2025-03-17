import helpers as h

import datetime

import pandas as pd
import geopandas as gpd
import uuid
import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine, text
from shapely import wkt
import multiprocessing as mp

# ======
# HELPER FUNCTIONS ============================================================
# ======

def connect_to_db():
    """Establishes and returns a connection to the local PostgreSQL database."""
    details = h.pg_details()
    details['dbname'] = 'blob_matching'
    try:
        conn = psycopg2.connect(**details)
        return conn
    except Exception as e:
        print("Error connecting to database:", e)
        raise

def get_table_data(table_name: str):
    """
    Fetches all records from the specified table.
    
    Args:
        table_name (str): The name of the table ('footprints' or 'blobs').
    
    Returns:
        list of dict: The rows from the table as a list of dictionaries.
    """
    conn = connect_to_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            query = f"SELECT * FROM {table_name}"
            cursor.execute(query)
            records = cursor.fetchall()
            return records
    except Exception as e:
        print(f"Error fetching data from table {table_name}:", e)
        raise
    finally:
        conn.close()

# ======
# Database Access Functions ===================================================
# ======

# database/commonDatabaseFunctions.py
def close_db_engine(engine=None):
    """
    To avoid having too many idle connections open
    """
    if engine != None:
        engine.dispose()
        engine = None

# database/commonDatabaseFunctions.py
# def query_db(conn = None, query: str = None):
    # """
    # _summary_
    
    # Args:
    #     query (_type_): _description_

    # Raises:
    #     e: _description_

    # Returns:
    #     _type_: _description_
    # """
    # # def query_db(query, db_conn_str=None):
    # #     conn = getDataBaseEngine(db_conn_str=db_conn_str)
    # #     df = pd.read_sql_query(query, conn)
    # #     close_db_engine(conn)
    # #     return df
    # if conn == None:
    #     conn = h.pg_details()
    # if query == None:
    #     query = """
    #     test
    #     """
    # df = pd.read_sql_query(query, conn)
    # close_db_engine(conn)
    # return df
    # 
    # """
    # Executes a SQL query and returns a DataFrame.
    
    # Args:
    #     query (str): The SQL query to execute.
    
    # Returns:
    #     DataFrame: The result as a pandas DataFrame.
    # """
    # conn = connect_to_db()  # use the connection helper
    # try:
    #     df = pd.read_sql_query(query, conn)
    #     return df
    # finally:
    #     conn.close()
def query_db(*args, **kwargs):
    """
    Executes a SQL query and returns a DataFrame using a SQLAlchemy engine.
    This override supports both the signatures:
      query_db(query)  and  query_db(conn, query)
      
    Args:
        Either a single positional argument (the SQL query as a string),
        or two positional arguments where the second is the SQL query.
        Also accepts a keyword argument "query".
    
    Returns:
        DataFrame: The result as a pandas DataFrame.
    """
    # Determine the query string
    if len(args) == 1:
        query_str = args[0]
    elif len(args) >= 2:
        query_str = args[1]
    else:
        query_str = kwargs.get("query", None)
    
    if query_str is None:
        raise ValueError("Query must be provided and not be None")
    
    details = h.pg_details()
    details['dbname'] = 'blob_matching'
    # Build connection string for SQLAlchemy
    conn_str = f"postgresql://{details['user']}:{details['password']}@{details['host']}:{details['port']}/{details['dbname']}"
    engine = create_engine(conn_str)
    try:
        # Wrap the query string with text()
        df = pd.read_sql_query(text(query_str), engine)
        return df
    finally:
        engine.dispose()

# database/commonDatabaseFunctions.py
def command_to_db(query, commit: bool = True):
    # """
    # Executes a SQL command like CREATE, INSERT, DELETE, UPDATE, etc.

    # Args:
    #     query (_type_): _description_
    #     commit (bool, optional): _description_. Defaults to True.

    # Raises:
    #     e: _description_

    # Returns:
    #     _type_: _description_
    # """
    # # def command_to_db(command, db_conn_str=None, commit=False):
    # # """
    # # Execute a SQL command like CREATE, INSERT, DELETE, UPDATE, etc.
    # # """
    # #     engine = getDataBaseEngine(db_conn_str=db_conn_str)
    # #     try:
    # #         with engine.connect() as connection:
    # #             if commit:
    # #                 with connection.begin():  # Use transaction for commands requiring commit
    # #                     result = connection.execute(command)
    # #                     # print("Rows affected:", result.rowcount)
    # #                     return result
    # #             else:
    # #                 result = connection.execute(command)
    # #                 return result
    # #     except Exception as e:
    # #         print(f"Error: Unable to execute the command. {e}")
    # #         print(traceback.format_exc())
    # #         raise e     # Raise the exception to the caller
    # engine = h.pg_details()
    # try:
    #     with engine.connect() as connection:
    #         if commit:
    #             with connection.begin():  # Use transaction for commands requiring commit
    #                 result = connection.execute(command)
                    
    #                 # print("Rows affected:", result.rowcount)
    #                 return result
    #         else:
    #             result = connection.execute(command)
    #             return result
    # except Exception as e:
    #     print(f"Error: Unable to execute the command. {e}")
    #     print(traceback.format_exc())
    #     raise e     # Raise the exception to the caller
    """
    Executes a SQL command (e.g., CREATE, INSERT, DELETE, UPDATE).
    
    Args:
        query (str): The SQL command to execute.
        commit (bool, optional): Whether to commit the transaction. Defaults to True.
    
    Returns:
        int: The number of affected rows.
    """
    conn = connect_to_db()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            if commit:
                conn.commit()
            return cursor.rowcount
    except Exception as e:
        print(f"Error executing command: {e}")
        conn.rollback()
        raise e
    finally:
        conn.close()


# ======
# Data Retrieval ==============================================================
# ======

# Data Retrieval (Fetching Polygons from the Database)
# BlobSearch/Helpers/BlobOverlappingFootprints.py
def get_data_by_year_month_place(year: int, month: int, place: str, place_type='city'):
    """
    Get data for a specific year, month, and city or county.
    
    Parameters:
    - year: The year to process.
    - month: The month to process.
    - place: The city or county to process.
    - place_type: 'CITY' or 'COUNTY'

    Returns:
    - A DataFrame with the BLOB data for the specified year, month, and place.
    """
    query = f"""SELECT "blob_id", "blob_polygon" 
                FROM "public"."blob" 
                WHERE "year" = '{year}' AND "month" = '{month}' AND "{place_type}" = '{place}'"""
    # query = f"""SELECT "BLOB_ID", "blob_polygon" 
    #             FROM "blob_matching"."blob" 
    #             WHERE "YEAR" = '{year}' AND "MONTH" = '{month}' AND "{place_type}" = '{place}'"""    

    df = query_db(query)
    df['blob_polygon'] = df['blob_polygon'].apply(wkt.loads)
    return df

# Data Retrieval (Fetching Polygons from the Database)
# BlobSearch/Helpers/BlobOverlappingFootprints.py
def get_footprint_data_by_place(place: str, place_type='city'):
    """
    Get footprint data based on city or county.

    Parameters:
    - place: The city or county to process.
    - place_type: 'city' or 'county'

    Returns:
    - A DataFrame with footprint data for the specified place.
    """
    if place_type == 'city':
        query = f"""SELECT "footprint_id",
                    "geometry" as "geometry"
                    FROM "public"."footprints" 
                    WHERE "{place_type}" = '{place}'"""
        # query = f"""SELECT "FOOTPRINT_ID",
        #     "GEOMETRY" as "geometry"
        #     FROM "blob_matching"."footprints" 
        #     WHERE "{place_type}" = '{place}'"""
    elif place_type == 'county':
        place = place.capitalize()
        place += ' County'
        query = f"""SELECT "footprint_id",
                    "geometry" as "geometry"
                    FROM "public"."footprints" 
                    WHERE "county_name" = '{place}'"""
        # query = f"""SELECT "FOOTPRINT_ID",
        #             "GEOMETRY" as "geometry"
        #             FROM "blob_matching"."footprints" 
        #             WHERE "COUNTY_NAME" = '{place}'"""

    df_footprints = query_db(query)
    df_footprints['geometry'] = df_footprints['geometry'].apply(wkt.loads)
    return df_footprints

# Data Retrieval (Fetching Polygons from the Database)
# BlobSearch/Helpers/BlobOverlappingFootprints.py
def get_footprint_data_by_cities(cities: list):
    """
    Get footprint data based on a list of cities.

    Parameters:
    - cities: A list of cities to process.

    Returns:
    - A DataFrame with footprint data for the specified cities.
    """

    escaped_cities = [escape_single_quotes(city) for city in cities if city != None]

    if len(escaped_cities) == 0:
        return pd.DataFrame()

    query = f"""SELECT "footprint_id",
                "geometry" as "geometry"
                FROM footprints
                WHERE "city" IN ({','.join(f"'{city}'" for city in escaped_cities)})"""

    df_footprints = query_db(query)
    df_footprints['geometry'] = df_footprints['geometry'].apply(wkt.loads)
    return df_footprints

# Data Retrieval (Fetching Polygons from the Database)
# BlobSearch/Helpers/BlobOverlappingFootprints.py
def get_distinct_places_from_footprints(place_type='city', table_name='blob', year=None, month=None):
    """
    Get distinct cities or counties from the footprints table that also exist in the blob table, 
    based on the place_type, and filter by year and month if provided.
    
    Parameters:
    - place_type: 'city' or 'county'
    - table_name: The name of the table to check for distinct places (default is 'blob').
    - year: Optional year to filter the results.
    - month: Optional month to filter the results.

    Returns:
    - A list of distinct cities or counties that are present in both the footprints and blob tables, 
      filtered by year and month if provided.
    """

    # Query distinct places from the blob table first
    blob_place_type = place_type if place_type != 'county' else 'county'  # Adjust place_type for blob

    query_blob = f'SELECT DISTINCT "{blob_place_type}" FROM "public"."{table_name}"'
    # query_blob = f'SELECT DISTINCT "{blob_place_type}" FROM "blob_matching"."{table_name}"'

    # Filter by year and month if provided
    conditions = []
    if year:
        conditions.append(f""""year" = '{year}'""")
    if month:
        
        conditions.append(f""""month" = '{month.zfill(2)}'""")

    if conditions:
        query_blob += f" WHERE {' AND '.join(conditions)}"
    
    df_blob = query_db(query_blob)

    # Adjust place_type for footprints table after blob query
    if place_type == 'COUNTY':
        place_type = 'COUNTY_NAME'

    # Query distinct places from the footprints table
    query_footprints = f'SELECT DISTINCT "{place_type}" FROM "public"."footprints"'
    # query_footprints = f'SELECT DISTINCT "{place_type}" FROM "blob_matching"."footprints"'
    df_footprints = query_db(query_footprints)

    # Find the intersection of places in both tables
    place_list = list(set(df_footprints[place_type]).intersection(set(df_blob[blob_place_type])))
    
    return place_list

# ======
# Polygon Matching ============================================================
# ======

# BlobSearch/BlobSearchBusinessClass.py
def match_property_between_months(curr_row, prev_data, return_dict={}, j=-1):
    """ Match curr_polygon with the bbox of the previous and previous previous month to find blob_ids that match """
    curr_polygon = make_polygon_valid(curr_row["blob_polygon"], curr_row["polygon_boundry_box"])
    if not curr_polygon:
        return []                                   # TODO: REMOVE RETURN
    
    if "is_imputed" in curr_row and curr_row["is_imputed"] and curr_row["is_imputed"] == True:
        prev_match = curr_row["previous_month_blob_ids"].split("|")
        return_dict[j] = prev_match
        return prev_match
    
    curr_polygon_area = curr_polygon.area

    if curr_polygon_area == 0:
        return []

    prev_match = []
    if prev_data is not None:
        matched_df = match_polygon_with_dataframe(curr_polygon, prev_data, "blob_polygon")
        if len(matched_df) > 0:
            prev_match += matched_df["blob_id"].values.tolist()

    return_dict[j] = prev_match

    return prev_match

# BlobSearch/BlobSearchBusinessClass.py
def match_properties_batched(curr_data, prev_data, return_dict={}, start_index=-1, end_index=-1):
    if end_index > len(curr_data):
        end_index = len(curr_data)
    for i in range(start_index, end_index):
        match_property_between_months(curr_data.iloc[i], prev_data, return_dict, i)
    return return_dict

# BlobSearch/BlobSearchBusinessClass.py
def match_properties_between_months(curr_data, prev_data):
    """ Match the property between the previous and current month based on the blob_polygon column field."""

    manager = mp.Manager()
    return_dict = manager.dict()
    jobs = []
        
    # Convert all string polygons to Shapely Polygon objects
    if isinstance(curr_data, pd.DataFrame) and len(curr_data) > 0:
        curr_data["matched_blob_ids"] = [None]*len(curr_data)
        if type(curr_data["blob_polygon"].iloc[0]) == str:
            curr_data["blob_polygon"] = curr_data["blob_polygon"].apply(wkt.loads)
    else:
        return pd.DataFrame()

    if isinstance(prev_data, pd.DataFrame) and len(prev_data) > 0:
        if type(prev_data["blob_polygon"].iloc[0]) == str:
            prev_data["blob_polygon"] = prev_data["blob_polygon"].apply(wkt.loads)
    else:
        return pd.DataFrame()
    

    # # OPTION 1: Multiprocess Individual
    # for i, row in tqdm(curr_data.iterrows(), desc=f"Matching Blobs ({len(curr_data)})", unit="iteration"):
    #     process = mp.Process(target=match_property_between_months, 
    #                          args=(row, prev_data, prev_prev_data, return_dict, i))
    #     jobs.append(process)
    #     process.start()

    # OPTION 2: Multiprocess in Batches - BEST
    max_parallel_processes = 50    # TODO: Revisit when we need to scale
    batch_size = int(len(curr_data)/max_parallel_processes)
    batch_size = max(batch_size, 1)
    # print(f"Batch Size: {batch_size}")
    # for i in tqdm(range(0, len(curr_data), batch_size), desc=f"Matching Blobs ({len(curr_data)})", unit="iteration"):
    for i in range(0, len(curr_data), batch_size):
        process = mp.Process(target=match_properties_batched, 
                             args=(curr_data, prev_data, return_dict, i, i+batch_size))
        jobs.append(process)
        process.start()
    

    for job in jobs:
        job.join()

    for i in return_dict.keys():
        if return_dict[i] != []:
            curr_data.loc[i, "matched_blob_ids"] = "|".join(return_dict[i])

    curr_data["num_matched"] = curr_data["matched_blob_ids"].apply(lambda x: len(str(x).split("|")) if x != None else 0)
    
    return curr_data

# Polygon Matching (Processing and Comparing Polygons)
# BlobSearch/MatchPolygons.py
# from app.BlobSearch.Helpers.BlobHelper import make_polygon_valid
def match_polygon_with_dataframe(polygon, polygon_dataframe, polygon_column_name="geometry", threshold=0.5):
    polygon = make_polygon_valid(polygon)
    if not polygon:
        return []
    if isinstance(polygon_dataframe, type(None)):
        return []
    if isinstance(polygon_dataframe, pd.DataFrame) and polygon_dataframe.empty:
        return []
    
    matched_idxs = []
    polygon_area = polygon.area
    for i, row in polygon_dataframe.iterrows():   
        other_polygon = make_polygon_valid(row[polygon_column_name])
        if not other_polygon:
            continue
        other_polygon_area = other_polygon.area
        if other_polygon_area == 0:
            continue
        try:
            if polygon.intersects(other_polygon):
                area_of_intersection = polygon.intersection(other_polygon).area
                if area_of_intersection/polygon_area > threshold or area_of_intersection/other_polygon_area > threshold:
                    matched_idxs.append(i)
        except Exception as e:
            print(f"Error in match_polygon_with_dataframe: {e}")
            print(f"\tdf Length: {len(polygon_dataframe)} | index: {i}")
            print(f"\tPolygon 1: {polygon}")
            print(f"\tPolygon 2: {other_polygon}")
            traceback.print_exc()
            continue
    return polygon_dataframe.loc[matched_idxs]

# Polygon Matching (Processing and Comparing Polygons)
# BlobSearch/Helpers/BlobHelper.py
def make_polygon_valid(polygon, backup_polygon=None):
    if isinstance(polygon, str):
        polygon = wkt.loads(polygon)
    new_polgon = polygon
    if not new_polgon.is_valid:
        new_polgon = polygon.buffer(0)
    if not new_polgon.is_valid:
        new_polgon = make_valid(polygon)
    if not new_polgon.is_valid:
        if isinstance(backup_polygon, str):
            backup_polygon = wkt.loads(backup_polygon)
        if backup_polygon.is_valid:
            return backup_polygon
        return None
    return new_polgon

# Polygon Matching (Processing and Comparing Polygons)
# BlobSearch/Helpers/BlobOverlappingFootprints.py
def exclude_footprints(df_city, df_fp):
    """
    Parameters:
    - df_city: DataFrame of city polygons (BLOB data)
    - df_fp: DataFrame of footprint polygons

    Returns:
    - A list of tuples (BLOB_ID, FOOTPRINT_ID) that need to be updated
    """
    # Convert the DataFrames into GeoDataFrames
    gdf_city = gpd.GeoDataFrame(df_city, geometry='blob_polygon')
    gdf_fp = gpd.GeoDataFrame(df_fp, geometry='geometry')
    # Perform spatial join
    df2 = gpd.sjoin(gdf_city, gdf_fp, how='left', predicate='intersects')
    # Drop duplicate BLOB_IDs
    df_drop_dupes = df2.drop_duplicates('blob_id')
    # Filter out rows where there is no intersection (i.e., NaN footprint data)
    df_no_null = df_drop_dupes[df_drop_dupes['index_right'].notna()]
    # Select relevant columns (BLOB_ID, FOOTPRINT_ID)
    final_df = df_no_null[['blob_id', 'footprint_id']]
    # Return as a list of tuples
    #final_df = final_df.head(20)
    blob_footprint_tuples = list(final_df.itertuples(index=False, name=None))
    
    return blob_footprint_tuples

# Polygon Matching (Processing and Comparing Polygons)
# BlobSearch/Helpers/BlobOverlappingFootprints.py
def process_year_month_place(year, month, place, place_type='city'):
    """
    Processes data by Year, Month, and City or County by downloading the relevant data, 
    comparing it with footprint data, and returning the BLOB_IDs that need to be updated.

    Parameters:
    - year: The year to process.
    - month: The month to process.
    - place: The city or county to process.
    - place_type: 'city' or 'county'

    Returns:
    - blob_ids: A list of BLOB_IDs to be updated.
    """
    try:
        start_time = datetime.datetime.now()
        print(f'Processing Year: {year}, Month: {month}, {place_type}: {place} at {start_time}...')

        # Download data for the given year, month, and place
        df_city = get_data_by_year_month_place(year, month, place, place_type)
        download_end = datetime.datetime.now()
        time_passed = download_end - start_time
        print(f'Year: {year}, Month: {month}, {place_type}: {place} took {time_passed} to download')

        # Download footprint data for the given place
        df_fp = get_footprint_data_by_place(place, place_type)

        # Exclude footprints and get blob_ids
        blob_ids = exclude_footprints(df_city, df_fp)
        print(f'Year: {year}, Month: {month}, {place_type}: {place}: {len(blob_ids)}')

        return blob_ids

    except Exception as e:
        print(f'Error processing Year: {year}, Month: {month}, {place_type}: {place}: {e}')
        return []
    
# ======
# Unmatching and Record Creation Functions ====================================
# ======

# BlobSearch/BlobSearchBusinessClass.py
def get_unmatched_blob_ids(matched_blobs, prev_blobs):
    """ Get the unmatched blobs from the previous month """
    if not (isinstance(prev_blobs, pd.DataFrame) and len(prev_blobs) > 0):
        return []
    
    prev_blob_ids = list(prev_blobs['blob_id'].values)
    prev_blob_ids = list(set(prev_blob_ids))
    
    matched_blob_ids = []
    for i, row in matched_blobs.iterrows():
        matched_blob_ids += str(row['matched_blob_ids']).split("|")
    matched_blob_ids = list(set(matched_blob_ids))
    unmatched_blob_ids = list(set(prev_blob_ids) - set(matched_blob_ids))

    return unmatched_blob_ids

# BlobSearch/BlobSearchBusinessClass.py
def create_unmatched_blob_records(unmatched_blob_ids, prev_blobs_df):
    if not (isinstance(prev_blobs_df, pd.DataFrame) and len(prev_blobs_df) > 0):
        return []
    
    blobs = []

    prev_yyyymm = prev_blobs_df["YEAR"].values[0] + prev_blobs_df["MONTH"].values[0]
    curr_yyyymm = get_next_yyyymm(prev_yyyymm)
    df = prev_blobs_df[prev_blobs_df["blob_id"].isin(unmatched_blob_ids)]
    df.set_index('BLOB_ID', inplace=True)   # Super fast for searching by BLOB_ID
    # for i, blob_id in tqdm(enumerate(unmatched_blob_ids), desc=f"Create Imputed Blobs ({len(unmatched_blob_ids)}) for {curr_yyyymm}", unit="iteration"):
    for i, blob_id in enumerate(unmatched_blob_ids):
        row = df.loc[blob_id] 
        blobs.append(BLOB(
            BLOB_ID="BID_"+str(uuid.uuid4()),
            YEAR=curr_yyyymm[:4],
            MONTH=curr_yyyymm[4:],

            CITY=str(row["city"]),
            STATE=str(row["state"]),
            CONSTRUCTION_STAGE=str(row["construction_stage"]),
            BUILDING_TYPE=str(row["building_type"]),
            CS_MODEL_ID=str(row["cs_model_id"]),
            BT_MODEL_ID=str(row["bt_model_id"]),

            POINT=str(row["point"]),
            SIZE=int(row["size"]),
            blob_polygon=str(row["blob_polygon"]),
            POLYGON_BOUNDRY_BOX=str(row["polygon_boundry_box"]),
            GEO_HASHES=str(row["geo_hashes"]),
            
            FULL_IMG_ID=str(row["full_img_id"]),
            PREVIOUS_MONTH_BLOB_IDS=blob_id,

            IS_IMPUTED=True,        # IMPORTANT FIELD
            IS_BLOB_ON_IMAGE_EDGE=bool(row["is_blob_on_image_edge"]),
            IS_VALID=bool(row["is_valid"]),
            IS_DUPLICATE=bool(row["is_duplicate"]),
        ))

    return blobs

# ======
# Update Results/Data Storage/Output ==========================================
# ======

# Data Storage/Output (Updating the Database with Results)
# BlobSearch/Helpers/BlobOverlappingFootprints.py
def update_table(table_name, col_name, blob_footprint_tuples, batch_size=1000):
    """
    Updates the specified table by setting IS_OVERLAPPING_FOOTPRINT to True 
    and updating FOOTPRINT_ID for the given BLOB_IDs in batches.
    """
    total_entries = len(blob_footprint_tuples)
    for i in range(0, total_entries, batch_size):
        batch = blob_footprint_tuples[i:i + batch_size]

        try:
            # Construct the CASE statement for the current batch
            update_cases = " ".join([f"WHEN '{blob_id}' THEN '{footprint_id}'"
                                     for blob_id, footprint_id in batch])

            # Create the final query
            query = f"""
                UPDATE "public"."{table_name}"
                SET "is_overlapping_footprint" = True,
                    "footprint_id" = CASE "{col_name}" {update_cases} ELSE "footprint_id" END
                WHERE "{col_name}" IN ({','.join(f"'{blob_id}'" for blob_id, _ in batch)});
            """
            
            command_to_db(query, commit=True)
            print(f'Data for {len(batch)} entries updated successfully in table {table_name}')
        except Exception as e:
            print(f'Error updating table {table_name} for batch starting at index {i}: {e}')
            print(traceback.format_exc())
            raise e     # Raise the exception to stop the process if an error occurs

# optional
# Data Storage/Output (Updating the Database with Results)
# BlobSearch/BlobSearchBusinessClass.py
def update_blob_bc_records(self, blob_bc_ids):
    if len(blob_bc_ids) == 0:
        print(f"No Blob Business Classes to Mark as Invalid")
        return False
    blob_bc_ids_str = "','".join(blob_bc_ids)
    command = f"""
    UPDATE BLOB_BUSINESS_CLASS
    SET "IS_VALID" = False
    WHERE "blob_business_class_id" IN ('{blob_bc_ids_str}')
    """
    command_to_db(command, commit=True)
    print(f"Updated {len(blob_bc_ids)} Business Classes as Invalid")
    return True

# ======
# Main block ==================================================================
# ======
if __name__ == "__main__":
    # Define the parameters for the match you want to run.
    # Adjust these values to match data available in your database.
    year = 2024
    month = "06"
    place = "Collin"  # or another city/county present in your data
    place_type = "county"  # "city" or "county" if applicable

    # Run the matching function that processes the specified year, month, and place.
    matches = process_year_month_place(year, month, place, place_type)

    # Output the result: this should print the list of blob-footprint tuples that were matched.
    print(f"Found {len(matches)} matching blob-footprint pairs:")
    for match in matches:
        print(match)
