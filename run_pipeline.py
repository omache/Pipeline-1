# run_pipeline.py
# Top-level script to run the entire address matching pipeline
# and collect performance metrics.

import time
import logging
import os
import sys
import subprocess # To run memory_profiler as a subprocess
from memory_profiler import profile # For inline memory profiling (less accurate for total process)

# Add src directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

from src import ingest, parse, match, fallback, report, simulate_data
from src.config import DB_CONFIG, SIMULATED_TRANSACTIONS_CSV, TRANSACTIONS_CSV

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Helper function to check database connection ---
def check_db_connection():
    """Checks if a connection to the database can be established."""
    logging.info(f"Attempting to connect to database: {DB_CONFIG.DB_NAME} at {DB_CONFIG.DB_HOST}:{DB_CONFIG.DB_PORT}")
    try:
        conn = ingest.get_db_connection() # Use the get_db_connection from ingest (or database.py)
        conn.close()
        logging.info("Database connection successful.")
        return True
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        logging.error("Please ensure the database is running and connection details in src/config.py or environment variables are correct.")
        return False

# --- Function to run a specific pipeline step ---
def run_step(step_name, step_function, *args, **kwargs):
    """Runs a pipeline step and logs its duration."""
    logging.info(f"--- Starting Step: {step_name} ---")
    start_time = time.time()
    try:
        step_function(*args, **kwargs)
        end_time = time.time()
        duration = end_time - start_time
        logging.info(f"--- Step Completed: {step_name} in {duration:.2f} seconds ---")
        return duration
    except Exception as e:
        logging.error(f"--- Step Failed: {step_name} with error: {e} ---")
        # Depending on requirements, you might want to exit or continue
        sys.exit(1) # Exit on critical step failure

# --- Function to run the full pipeline ---
# @profile # Uncomment this line to profile memory of this function (might add overhead)
def run_full_pipeline(use_simulated_data=False):
    """Runs the full address matching pipeline."""
    logging.info("--- Running Full Address Matching Pipeline ---")

    if not check_db_connection():
        logging.error("Cannot proceed without a database connection.")
        sys.exit(1)

    total_runtime = 0

    # Determine which transaction data to use
    transactions_input_csv = SIMULATED_TRANSACTIONS_CSV if use_simulated_data else TRANSACTIONS_CSV
    if use_simulated_data and not os.path.exists(transactions_input_csv):
        logging.warning(f"Simulated data file not found at {transactions_input_csv}. Running simulation first.")
        run_step("Data Simulation", simulate_data.simulate_large_transactions_csv)
        # Re-check if file exists after simulation
        if not os.path.exists(transactions_input_csv):
             logging.error(f"Simulated data file still not found after running simulation script: {transactions_input_csv}")
             sys.exit(1)


    # Step 1: Ingestion
    # Optional: Clear existing data before ingestion for a clean run
    logging.info("Clearing existing data before ingestion...")
    conn = ingest.get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM transactions;")
    cur.execute("DELETE FROM canonical_addresses;")
    conn.commit()
    conn.close()
    logging.info("Existing data cleared.")

    total_runtime += run_step("Ingest Canonical Addresses", ingest.ingest_canonical_addresses)
    total_runtime += run_step("Ingest Transactions", ingest.ingest_transactions, csv_path=transactions_input_csv) # Use selected CSV

    # Step 2: Parsing and Normalization
    total_runtime += run_step("Parse and Normalize Addresses", parse.parse_all_transactions)

    # Step 3: Matching
    total_runtime += run_step("Exact Matching", match.perform_exact_matching)
    total_runtime += run_step("Fuzzy Matching", match.perform_fuzzy_matching)
    # If phonetic matching is a separate step before fallback:
    # total_runtime += run_step("Phonetic Matching", match.perform_phonetic_matching)


    # Step 4: Fallback (including API)
    total_runtime += run_step("Fallback Matching (API, etc.)", fallback.perform_fallback_matching)

    # Step 5: Reporting
    total_runtime += run_step("Generate Final Output Report", report.generate_final_output_csv)
    total_runtime += run_step("Generate Unmatched Report", report.generate_unmatched_report_csv)


    logging.info("--- Pipeline Execution Completed ---")
    logging.info(f"Total Pipeline Runtime (excluding simulation if run separately): {total_runtime:.2f} seconds")

    # Placeholder for Peak Memory and Cost Reporting
    logging.info("\n--- Performance Metrics ---")
    logging.info(f"Total Runtime: {total_runtime:.2f} seconds")
    logging.info("Peak Memory: [Monitor using OS tools or memory_profiler]")
    logging.info("Approximate Cost (if using real API): [Estimate based on API calls]")
    logging.info("---------------------------")


if __name__ == "__main__":
    use_simulated_data = '--simulate' in sys.argv
    if use_simulated_data:
        logging.info("Running pipeline with simulated large dataset.")
        run_full_pipeline(use_simulated_data=True)
    else:
        logging.info("Running pipeline with original small dataset.")
        run_full_pipeline(use_simulated_data=False)

