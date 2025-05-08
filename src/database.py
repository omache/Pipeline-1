# src/database.py
# Database connection and utility functions.

import psycopg2
from src.config import DB_CONFIG
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection():
    """Establishes and returns a database connection."""
    try:
        conn = psycopg2.connect(DB_CONFIG.connection_string)
        logging.info("Database connection established successfully.")
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Database connection failed: {e}")
        # Depending on the application, you might want to re-raise the exception
        # or handle it differently.
        raise

def close_db_connection(conn):
    """Closes a database connection."""
    if conn:
        conn.close()
        logging.info("Database connection closed.")

def execute_query(query, params=None, fetchone=False, fetchall=False):
    """
    Executes a SQL query.

    Args:
        query (str): The SQL query string.
        params (tuple, optional): Parameters for the query. Defaults to None.
        fetchone (bool, optional): If True, fetches one row. Defaults to False.
        fetchall (bool, optional): If True, fetches all rows. Defaults to False.

    Returns:
        list or tuple or None: Query results based on fetchone/fetchall flags.
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(query, params)
        conn.commit() # Commit changes for INSERT, UPDATE, DELETE

        if fetchone:
            return cur.fetchone()
        elif fetchall:
            return cur.fetchall()
        else:
            return None # For queries that don't return data (INSERT, UPDATE, DELETE)

    except psycopg2.Error as e:
        logging.error(f"Database query failed: {e}\nQuery: {query}")
        if conn:
            conn.rollback() # Rollback changes on error
        raise # Re-raise the exception after logging

    finally:
        if conn:
            close_db_connection(conn)

def execute_batch(query, data):
    """
    Executes a query in batches using execute_values for efficiency.

    Args:
        query (str): The SQL query string with placeholders (e.g., INSERT ... VALUES (%s, %s, %s)).
        data (list of tuples): A list of tuples, where each tuple is a row of data.
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # Use execute_values for efficient bulk insertion/updates
        psycopg2.extras.execute_values(cur, query, data)
        conn.commit()
        logging.info(f"Batch execution successful for query: {query[:50]}...") # Log first 50 chars
    except psycopg2.Error as e:
        logging.error(f"Database batch execution failed: {e}\nQuery: {query[:50]}...")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            close_db_connection(conn)

