# src/ingest.py
# Script to ingest data from CSV files into the PostgreSQL database,
# corrected to match the provided schema and table names.

import pandas as pd
from src.database import get_db_connection, close_db_connection
from src.config import CANONICAL_ADDRESSES_CSV, TRANSACTIONS_CSV # Assuming these are defined
import logging
import psycopg2.extras # Required for execute_values
import time
# Assume PostGIS is enabled if GEOMETRY is used in schema

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def ingest_canonical_addresses(csv_path=CANONICAL_ADDRESSES_CSV):
    """
    Ingests canonical addresses from a CSV file into the database,
    matching the canonical_addresses schema (based on 'Address' headers).
    Optimized for faster ingestion.

    Args:
        csv_path (str): Path to the canonical addresses CSV file.
    """
    logging.info(f"Starting ingestion of canonical addresses from {csv_path}")
    conn = None
    
    try:
        # Use Pandas' chunksize parameter to read CSV in chunks to handle large files
        chunk_size = 50000
        csv_chunks = pd.read_csv(csv_path, sep=',', chunksize=chunk_size, low_memory=False) # low_memory=False can help with mixed types
        
        # Connect to DB once before processing chunks
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Define expected columns for the database table
        # IMPORTANT: This order must match the INSERT INTO ... VALUES statement
        db_columns = [
            'hhid', 'fname', 'mname', 'lname', 'suffix', 'address', 'house', 
            'predir', 'street', 'strtype', 'postdir', 'apttype', 'aptnbr', 
            'city', 'state', 'zip', 'latitude', 'longitude', 'homeownercd'
        ]
        
        total_rows = 0
        for chunk_idx, df in enumerate(csv_chunks):
            chunk_start_time = time.time()
            logging.info(f"Processing canonical_addresses chunk {chunk_idx+1} with {len(df)} rows")
            
            # Clean zip codes
            df['zip_clean'] = df['zip'].apply(
                lambda x: str(int(x)) if pd.notna(x) and isinstance(x, (int, float))
                else str(x).strip() if pd.notna(x) else None
            )
            
            # Ensure all database columns are present in the DataFrame chunk
            # Add missing columns with None if they don't exist in the CSV chunk
            for col in db_columns:
                if col not in df.columns and col != 'zip': # 'zip' is handled by 'zip_clean'
                    df[col] = None
                elif col == 'zip' and 'zip_clean' in df.columns : # map zip_clean to zip for insertion
                    df['zip'] = df['zip_clean']

            # Select and reorder columns to match db_columns for insertion
            # This handles cases where CSV columns are in a different order or have different names
            # that were not aliased earlier.
            # For this specific function, it seems CSV columns match db_columns directly (except 'zip' vs 'zip_clean')
            
            # Ensure df has all db_columns in the correct order
            df_ordered = df.reindex(columns=db_columns)

            # Extract data as list of tuples - much faster than iterrows()
            data = list(df_ordered.itertuples(index=False, name=None))
            
            # Insert query to match canonical_addresses schema columns (excluding address_id)
            insert_query_template = f"""
            INSERT INTO canonical_addresses ({', '.join(db_columns)})
            VALUES %s;
            """
            
            optimal_page_size = min(len(data), 10000) # Adjusted default, can be tuned
            psycopg2.extras.execute_values(cur, insert_query_template, data, page_size=optimal_page_size)
            
            total_rows += len(data)
            chunk_time = time.time() - chunk_start_time
            logging.info(f"Canonical addresses chunk {chunk_idx+1} processed in {chunk_time:.2f} seconds ({len(data)/chunk_time if chunk_time > 0 else 0:.2f} rows/sec)")

        conn.commit()
        logging.info(f"Successfully ingested {total_rows} canonical addresses in total")
        
    except FileNotFoundError:
        logging.error(f"Error: Canonical addresses CSV not found at {csv_path}")
        raise
    except psycopg2.Error as e:
        logging.error(f"Database ingestion failed for canonical addresses: {e}")
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        logging.error(f"Unexpected error during canonical address ingestion: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            close_db_connection(conn)


def ingest_transactions(csv_path=TRANSACTIONS_CSV):
    """
    Ingests transaction data from a CSV file into the database,
    matching the transactions schema. Optimized for speed.

    Args:
        csv_path (str): Path to the transactions CSV file.
    """
    logging.info(f"Starting ingestion of transactions from {csv_path}")
    conn = None
    try:
        chunk_size = 50000  # Process in chunks
        csv_chunks = pd.read_csv(csv_path, sep=',', chunksize=chunk_size, low_memory=False)

        conn = get_db_connection()
        cur = conn.cursor()

        # Define the order of columns as they appear in the transactions table
        # This order MUST match the INSERT INTO ... VALUES statement
        db_columns = [
            'id', 'status', 'price', 'bedrooms', 'bathrooms', 'square_feet', 
            'address_line_1', 'address_line_2', 'city', 'state', 'zip_code', 
            'property_type', 'year_built', 'presented_by', 'brokered_by', 
            'presented_by_mobile', 'mls', 'listing_office_id', 'listing_agent_id', 
            'created_at', 'updated_at', 'open_house', 'latitude', 'longitude', 'email', 
            'list_date', 'pending_date', 'presented_by_first_name', 
            'presented_by_last_name', 'presented_by_middle_name', 'presented_by_suffix', 
            'geog' # Placeholder for geog, will be handled separately or via ST_GeomFromText
        ]
        
        # Columns that are dates and need parsing
        date_cols_to_parse = ['created_at', 'updated_at', 'list_date', 'pending_date']
        
        total_rows = 0
        ids_for_geog_update = [] # To store primary keys of rows needing geog update

        for chunk_idx, df_chunk in enumerate(csv_chunks):
            chunk_start_time = time.time()
            logging.info(f"Processing transactions chunk {chunk_idx + 1} with {len(df_chunk)} rows")

            # 1. Standardize column names (if necessary, e.g., 'zip' to 'zip_code')
            # Assuming CSV column names mostly match db_columns. If not, rename them:
            # df_chunk.rename(columns={'old_name': 'new_name'}, inplace=True)

            # 2. Handle Date Parsing (Vectorized)
            for col in date_cols_to_parse:
                if col in df_chunk.columns:
                    # Try specific format first, then fallback
                    try:
                        df_chunk[col] = pd.to_datetime(df_chunk[col], format='%m/%d/%Y', errors='coerce')
                    except (ValueError, TypeError): # Catches more issues if format is very varied
                        df_chunk[col] = pd.to_datetime(df_chunk[col], errors='coerce')
                    # Convert NaT to None for database compatibility
                    df_chunk[col] = df_chunk[col].apply(lambda x: None if pd.isna(x) else x)


            # 3. Clean Zip Codes (Vectorized)
            if 'zip_code' in df_chunk.columns:
                df_chunk['zip_code'] = df_chunk['zip_code'].apply(
                    lambda x: str(int(x)) if pd.notna(x) and isinstance(x, (int, float))
                    else str(x).strip() if pd.notna(x) else None
                )
            else:
                 df_chunk['zip_code'] = None # Ensure column exists

            # 4. Prepare 'geog' data
            # We will insert WKT strings for PostGIS to convert.
            # ST_GeomFromText(%s, 4326) will be used in the query template for the 'geog' column value.
            # Alternatively, insert NULL and update later if ST_MakePoint(lon, lat) is strictly required.
            # For this optimization, we aim to do it in one INSERT.

            df_chunk['geog_wkt'] = None # Initialize column for WKT strings
            if 'latitude' in df_chunk.columns and 'longitude' in df_chunk.columns:
                # Create WKT strings only where lat and lon are not null
                valid_lat_lon = pd.notna(df_chunk['latitude']) & pd.notna(df_chunk['longitude'])
                df_chunk.loc[valid_lat_lon, 'geog_wkt'] = 'SRID=4326;POINT(' + df_chunk['longitude'].astype(str) + ' ' + df_chunk['latitude'].astype(str) + ')'
                
                # If 'geog' column also exists in CSV and has data, prioritize it if needed or handle conflicts
                if 'geog' in df_chunk.columns and not df_chunk['geog'].isnull().all():
                    # Example: prioritize existing 'geog' if it's not null, otherwise use lat/lon
                    df_chunk['geog_wkt'] = df_chunk['geog'].fillna(df_chunk['geog_wkt'])


            # 5. Ensure all db_columns are present and in order
            # Add missing columns from db_columns list with None
            for col in db_columns:
                if col not in df_chunk.columns:
                    if col == 'geog': # geog is handled by geog_wkt now for insertion
                        df_chunk[col] = df_chunk['geog_wkt'] # Use the prepared WKT string
                    else:
                        df_chunk[col] = None
                elif col == 'geog': # If 'geog' was already there but we want to use WKT
                     df_chunk[col] = df_chunk['geog_wkt']


            # Reorder/select columns to match the db_columns list
            df_ordered = df_chunk.reindex(columns=db_columns)
            
            # Convert DataFrame to list of tuples for execute_values
            data_tuples = list(df_ordered.itertuples(index=False, name=None))

            if not data_tuples:
                logging.info(f"Transactions chunk {chunk_idx + 1} is empty after processing. Skipping.")
                continue

            # 6. Build Insert Query
            # For geog, we will let PostGIS convert WKT, or expect a direct EWKT string if provided.
            # The 'geog' column in data_tuples should now contain the WKT string or None.
            insert_query_template = f"""
            INSERT INTO transactions ({', '.join(db_columns)})
            VALUES %s
            ON CONFLICT (id) DO NOTHING; 
            """ 
            # Added ON CONFLICT (id) DO NOTHING as an example, if 'id' is a primary key
            # and you want to skip duplicates. Remove if not applicable or use DO UPDATE.

            # 7. Execute Batch Insert
            optimal_page_size = min(len(data_tuples), 1000) # Smaller page size might be better with 'geog'
            psycopg2.extras.execute_values(cur, insert_query_template, data_tuples, page_size=optimal_page_size)
            
            total_rows += len(data_tuples)
            chunk_time = time.time() - chunk_start_time
            logging.info(f"Transactions chunk {chunk_idx + 1} processed in {chunk_time:.2f} seconds ({len(data_tuples)/chunk_time if chunk_time > 0 else 0:.2f} rows/sec)")

        conn.commit()
        logging.info(f"Successfully ingested {total_rows} transactions.")

    except FileNotFoundError:
        logging.error(f"Error: Transactions CSV not found at {csv_path}")
        raise
    except psycopg2.Error as e:
        logging.error(f"Database ingestion failed for transactions: {e}")
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        logging.error(f"Unexpected error during transactions ingestion: {e}")
        if conn: # Ensure rollback on generic exception too
            conn.rollback()
        raise
    finally:
        if conn:
            close_db_connection(conn)


if __name__ == "__main__":
    conn = None
    try:
        logging.info("Clearing existing data from canonical_addresses and transactions...")
        conn = get_db_connection()
        cur = conn.cursor()
        # Clear data from tables this script ingests into
        # Use TRUNCATE for faster clearing if there are no foreign key constraints
        # or if you handle cascading. DELETE is safer but slower on huge tables.
        cur.execute("TRUNCATE TABLE transactions RESTART IDENTITY CASCADE;") # Be careful with CASCADE
        cur.execute("TRUNCATE TABLE canonical_addresses RESTART IDENTITY CASCADE;") # Be careful with CASCADE
        # If TRUNCATE is too risky or not allowed due to FKs:
        # cur.execute("DELETE FROM transactions;")
        # cur.execute("DELETE FROM canonical_addresses;")
        conn.commit()
        logging.info("Existing data from canonical_addresses and transactions cleared.")

        start_time_total = time.time()

        start_time_addresses = time.time()
        ingest_canonical_addresses()
        logging.info(f"Canonical addresses ingestion took {time.time() - start_time_addresses:.2f} seconds.")

        start_time_transactions = time.time()
        ingest_transactions()
        logging.info(f"Transactions ingestion took {time.time() - start_time_transactions:.2f} seconds.")
        
        logging.info(f"Total ingestion process completed in {time.time() - start_time_total:.2f} seconds.")

    except Exception as e:
        logging.error(f"Ingestion script failed: {e}")
    finally:
        if conn:
            close_db_connection(conn)