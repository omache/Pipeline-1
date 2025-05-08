# src/report.py
# Module for generating the final output CSV and the unmatched report.

import pandas as pd
from src.database import get_db_connection, close_db_connection
from src.config import FINAL_OUTPUT_CSV, UNMATCHED_REPORT_CSV
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_final_output_csv(output_path=FINAL_OUTPUT_CSV):
    """
    Generates the final output CSV containing matched transaction details.

    Args:
        output_path (str): Path to save the output CSV file.
    """
    logging.info(f"Generating final output report to {output_path}")

    conn = None
    try:
        conn = get_db_connection()
        query = """
        SELECT
            id AS transaction_id,  -- Changed from transaction_id to id (schema)
            matched_address_id,
            confidence_score,
            match_type
        FROM transactions;
        """

        # Use pandas to read the query results directly into a DataFrame
        df = pd.read_sql(query, conn)

        # Ensure output directory exists
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir): # Ensure output_dir is not empty
            os.makedirs(output_dir)
            logging.info(f"Created output directory: {output_dir}")

        # Save the DataFrame to a CSV file
        df.to_csv(output_path, index=False)

        logging.info(f"Final output report generated successfully with {len(df)} rows.")

    except Exception as e:
        logging.error(f"Error generating final output report: {e}")
        raise
    finally:
        if conn:
            close_db_connection(conn)

def generate_unmatched_report_csv(output_path=UNMATCHED_REPORT_CSV):
    """
    Generates a report of unmatched records and their reasons.

    Args:
        output_path (str): Path to save the unmatched report CSV file.
    """
    logging.info(f"Generating unmatched report to {output_path}")

    conn = None
    try:
        conn = get_db_connection()
        query = """
        SELECT
            id AS transaction_id,  -- Changed from transaction_id to id (schema)
            TRIM(
                COALESCE(address_line_1, '') ||
                CASE WHEN address_line_2 IS NOT NULL AND address_line_2 <> '' THEN ' ' || address_line_2 ELSE '' END ||
                CASE WHEN city IS NOT NULL AND city <> '' THEN ', ' || city ELSE '' END ||
                CASE WHEN state IS NOT NULL AND state <> '' THEN ', ' || state ELSE '' END ||
                CASE WHEN zip_code IS NOT NULL AND zip_code <> '' THEN ' ' || zip_code ELSE '' END
            ) AS raw_address, -- Constructed raw_address from schema columns
            unmatch_reason
        FROM transactions
        WHERE matched_address_id IS NULL; -- Records that are still unmatched
        """

        # Use pandas to read the query results directly into a DataFrame
        df = pd.read_sql(query, conn)

        # Ensure output directory exists
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir): # Ensure output_dir is not empty
            os.makedirs(output_dir) 
            logging.info(f"Created output directory: {output_dir}")


        # Save the DataFrame to a CSV file
        df.to_csv(output_path, index=False)

        logging.info(f"Unmatched report generated successfully with {len(df)} rows.")

    except Exception as e:
        logging.error(f"Error generating unmatched report: {e}")
        raise
    finally:
        if conn:
            close_db_connection(conn)

if __name__ == "__main__":
    try:
        # Ensure output directory exists for the example if running standalone
        if not os.path.exists("output"):
             os.makedirs("output")
        
        logging.info("Reporting script finished (example call).")
        logging.warning("Actual report generation requires database connection and processed data.")

    except Exception as e:
        logging.error(f"Reporting script failed: {e}")