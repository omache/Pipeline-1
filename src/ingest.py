# src/ingest.py

import pandas as pd
from src.database import get_db_connection, close_db_connection
from src.config import CANONICAL_ADDRESSES_CSV, TRANSACTIONS_CSV # Assuming these are defined
import logging
import psycopg2.extras # Required for execute_values
import time
# Removed import psutil

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def ingest_canonical_addresses(csv_path=CANONICAL_ADDRESSES_CSV):
    """
    Ingests canonical address data from a CSV file into the database.
    Uses chunking and batch insertion for performance.

    Args:
        csv_path (str): Path to the canonical addresses CSV file.
    """
    logging.info(f"Starting ingestion of canonical addresses from {csv_path}")
    conn = None

    try:
        chunk_size = 10000
        # Use iterator=True with chunksize for better memory management with large files
        csv_chunks = pd.read_csv(csv_path, sep=',', chunksize=chunk_size, low_memory=False, iterator=True)
        conn = get_db_connection()
        cur = conn.cursor()

        db_columns = [
            'hhid', 'fname', 'mname', 'lname', 'suffix', 'address', 'house',
            'predir', 'street', 'strtype', 'postdir', 'apttype', 'aptnbr',
            'city', 'state', 'zip', 'latitude', 'longitude', 'homeownercd'
        ]

        total_rows = 0
        for chunk_idx, df in enumerate(csv_chunks):
            chunk_start_time = time.time()
            logging.info(f"Processing canonical_addresses chunk {chunk_idx+1} with {len(df)} rows")

            # Ensure columns match and order is correct, add None for missing DB columns
            df_ordered = df.reindex(columns=db_columns)
            # Convert NaN in numeric columns to None for db insertion
            for col in ['latitude', 'longitude']: # Add other potential numeric cols if needed
                 if col in df_ordered.columns:
                     df_ordered[col] = df_ordered[col].apply(lambda x: None if pd.isna(x) else x)


            data = list(df_ordered.itertuples(index=False, name=None))

            if not data:
                logging.info(f"Canonical addresses chunk {chunk_idx+1} is empty after processing. Skipping.")
                continue

            insert_query_template = f"""
            INSERT INTO canonical_addresses ({', '.join(db_columns)})
            VALUES %s;
            """

            # Use a reasonable page size, min with len(data) is good
            optimal_page_size = min(len(data), 10000)
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
        chunk_size = 50000
        # Use iterator=True with chunksize
        csv_chunks = pd.read_csv(csv_path, sep=',', chunksize=chunk_size, low_memory=False, iterator=True)

        conn = get_db_connection()
        cur = conn.cursor()

        db_columns = [
            'id', 'status', 'price', 'bedrooms', 'bathrooms', 'square_feet',
            'address_line_1', 'address_line_2', 'city', 'state', 'zip_code',
            'property_type', 'year_built', 'presented_by', 'brokered_by',
            'presented_by_mobile', 'mls', 'listing_office_id', 'listing_agent_id',
            'created_at', 'updated_at', 'open_house', 'latitude', 'longitude', 'email',
            'list_date', 'pending_date', 'presented_by_first_name',
            'presented_by_last_name', 'presented_by_middle_name', 'presented_by_suffix',
            'geog'
        ]

        # Map CSV columns to DB columns explicitly if needed, or assume direct mapping
        # If CSV column names don't exactly match db_columns, you'd need a mapping step here.

        date_cols_to_parse = ['created_at', 'updated_at', 'list_date', 'pending_date']
        # Numeric columns that might contain NaNs needing conversion to None
        numeric_cols_to_clean = ['price', 'bedrooms', 'bathrooms', 'square_feet',
                                 'year_built', 'latitude', 'longitude'] # Add others as needed

        total_rows = 0

        for chunk_idx, df_chunk in enumerate(csv_chunks):
            chunk_start_time = time.time()
            logging.info(f"Processing transactions chunk {chunk_idx + 1} with {len(df_chunk)} rows")

            df_processed = df_chunk.reindex(columns=db_columns + ['geog_wkt']) # Include geog_wkt temporarily

            # Date parsing and NaT to None conversion
            for col in date_cols_to_parse:
                if col in df_processed.columns:
                    try:
                        df_processed[col] = pd.to_datetime(df_processed[col], format='%m/%d/%Y', errors='coerce')
                    except (ValueError, TypeError):
                        df_processed[col] = pd.to_datetime(df_processed[col], errors='coerce')
                    df_processed[col] = df_processed[col].replace({pd.NaT: None})

            # Convert NaN in numeric columns to None
            for col in numeric_cols_to_clean:
                if col in df_processed.columns:
                     df_processed[col] = df_processed[col].apply(lambda x: None if pd.isna(x) else x)


            # Prepare 'geog' data: Create WKT strings from lat/lon for PostGIS
            # Use the cleaned latitude/longitude columns
            df_processed['geog'] = None # Initialize geog column
            valid_lat_lon = pd.notna(df_processed['latitude']) & pd.notna(df_processed['longitude'])
            # Only apply if latitude and longitude exist in the chunk and are not None/NaN
            if 'latitude' in df_processed.columns and 'longitude' in df_processed.columns:
                 df_processed.loc[valid_lat_lon, 'geog'] = 'SRID=4326;POINT(' + df_processed['longitude'].astype(str) + ' ' + df_processed['latitude'].astype(str) + ')'


            # Select only the final db_columns for the tuple conversion
            df_final_data = df_processed[db_columns]

            data_tuples = list(df_final_data.itertuples(index=False, name=None))

            if not data_tuples:
                logging.info(f"Transactions chunk {chunk_idx + 1} is empty after processing. Skipping.")
                continue

            # Build Insert Query
            insert_query_template = f"""
            INSERT INTO transactions ({', '.join(db_columns)})
            VALUES %s
            ON CONFLICT (id) DO NOTHING;
            """
            # ON CONFLICT (id) DO NOTHING skips rows with duplicate 'id' primary keys.

            # Execute Batch Insert
            optimal_page_size = min(len(data_tuples), 50000) # Adjust page size based on data size/complexity
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
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            close_db_connection(conn)

def main():
    conn = None
    try:
        logging.info("Clearing existing data from canonical_addresses and transactions...")
        conn = get_db_connection()
        cur = conn.cursor()
        # Clear data from tables this script ingests into
        cur.execute("TRUNCATE TABLE transactions RESTART IDENTITY CASCADE;")
        cur.execute("TRUNCATE TABLE canonical_addresses RESTART IDENTITY CASCADE;")
        conn.commit()
        logging.info("Existing data from canonical_addresses and transactions cleared.")

        start_time_total = time.time()

        # Ingest Canonical Addresses
        start_time_addresses = time.time()
        ingest_canonical_addresses()
        addresses_ingestion_time = time.time() - start_time_addresses
        logging.info(f"Canonical addresses ingestion took {addresses_ingestion_time:.2f} seconds.")

        # Ingest Transactions
        start_time_transactions = time.time()
        ingest_transactions()
        transactions_ingestion_time = time.time() - start_time_transactions
        logging.info(f"Transactions ingestion took {transactions_ingestion_time:.2f} seconds.")

        total_ingestion_time = time.time() - start_time_total
        logging.info(f"Total ingestion process completed in {total_ingestion_time:.2f} seconds.")

    except Exception as e:
        logging.error(f"Ingestion script failed: {e}")
    finally:
        if conn:
            close_db_connection(conn)

if __name__ == "__main__":
    main()