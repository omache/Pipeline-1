# run_pipeline.py

import time
import logging
import os
import sys
import subprocess
import psutil
from memory_profiler import profile, memory_usage

# Add src directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

from src import ingest, parse, match, fallback, report, simulate_data
from src.config import DB_CONFIG, SIMULATED_TRANSACTIONS_CSV, TRANSACTIONS_CSV

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Helper function to check database connection ---
def check_db_connection():
    """Checks if a connection to the database can be established."""
    logging.info(f"Attempting to connect to database: {DB_CONFIG.DB_NAME} at {DB_CONFIG.DB_HOST}:{DB_CONFIG.DB_PORT}")
    try:
        conn = ingest.get_db_connection()
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
    
    # Get memory before execution
    process = psutil.Process(os.getpid())
    mem_before = process.memory_info().rss / 1024 / 1024 
    
    try:
        step_function(*args, **kwargs)
        end_time = time.time()
        duration = end_time - start_time
        
        # Get memory after execution
        mem_after = process.memory_info().rss / 1024 / 1024 
        mem_diff = mem_after - mem_before
        
        logging.info(f"--- Step Completed: {step_name} in {duration:.2f} seconds ---")
        logging.info(f"--- Memory Usage: {mem_after:.2f} MB (Δ: {mem_diff:+.2f} MB) ---")
        return duration
    except Exception as e:
        logging.error(f"--- Step Failed: {step_name} with error: {e} ---")
        sys.exit(1)

# --- Function to run the full pipeline ---
@profile
def run_full_pipeline(use_simulated_data=False):
    """Runs the full address matching pipeline."""
    logging.info("--- Running Full Address Matching Pipeline ---")

    if not check_db_connection():
        logging.error("Cannot proceed without a database connection.")
        sys.exit(1)

    total_runtime = 0
    
    # Get initial memory usage
    process = psutil.Process(os.getpid())
    initial_memory = process.memory_info().rss / 1024 / 1024  # Convert to MB

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
    logging.info("Clearing existing data before ingestion...")
    try:
        conn = ingest.get_db_connection()
        cur = conn.cursor()
        
        cur.execute("TRUNCATE TABLE transactions;")
        cur.execute("TRUNCATE TABLE canonical_addresses;")
        conn.commit()
        conn.close()
        logging.info("Existing data cleared.")
    except Exception as e:
        logging.error(f"Error clearing data: {e}")
        sys.exit(1)

    total_runtime += run_step("Ingest Canonical Addresses", ingest.ingest_canonical_addresses)
    total_runtime += run_step("Ingest Transactions", ingest.ingest_transactions, csv_path=transactions_input_csv) # Use selected CSV

    # Step 2: Parsing and Normalization
    total_runtime += run_step("Parse and Normalize Addresses", parse.parse_all_transactions)

    # Step 3: Matching
    total_runtime += run_step("Exact Matching", match.perform_exact_matching)
    total_runtime += run_step("Fuzzy Matching", match.perform_fuzzy_matching)
    # total_runtime += run_step("Phonetic Matching", match.perform_phonetic_matching)

    # Step 4: Fallback (including API)
    total_runtime += run_step("Fallback Matching (API, etc.)", fallback.perform_fallback_matching)

    # Step 5: Reporting
    total_runtime += run_step("Generate Final Output Report", report.generate_final_output_csv)
    total_runtime += run_step("Generate Unmatched Report", report.generate_unmatched_report_csv)

    # Get final memory usage
    final_memory = process.memory_info().rss / 1024 / 1024  # Convert to MB
    memory_increase = final_memory - initial_memory

    logging.info("--- Pipeline Execution Completed ---")
    logging.info(f"Total Pipeline Runtime (excluding simulation if run separately): {total_runtime:.2f} seconds")

    logging.info("\n--- Performance Metrics ---")
    logging.info(f"Total Runtime: {total_runtime:.2f} seconds")
    logging.info(f"Current Memory Usage: {final_memory:.2f} MB")
    logging.info(f"Memory Growth During Execution: {memory_increase:.2f} MB")
    logging.info("---------------------------")

def monitor_memory_usage():
    """Function to monitor memory usage of the current process."""
    process = psutil.Process(os.getpid())
    max_memory = 0
    
    while True:
        try:
            current_memory = process.memory_info().rss / 1024 / 1024  # MB
            max_memory = max(max_memory, current_memory)
            time.sleep(0.1)  # Check every 100ms
        except:
            break
    
    return max_memory

if __name__ == "__main__":
    if '--profile-memory' in sys.argv:
        logging.info("Running script with memory profiling...")
        use_simulated_data = '--simulate' in sys.argv
        
        try:
            logging.info("Starting memory profiling...")
            
            def run_profiled():
                return run_full_pipeline(use_simulated_data=use_simulated_data)
            
            mem_usage = memory_usage((run_profiled, tuple(), dict()), 
                                    interval=0.1, 
                                    timeout=None,
                                    include_children=True,
                                    max_iterations=None,
                                    retval=True)
            
            memory_measurements, function_result = mem_usage
            
            if memory_measurements and isinstance(memory_measurements, (list, tuple)):
                peak_memory = max(memory_measurements)
                logging.info(f"Peak Memory Usage: {peak_memory:.2f} MB")
                
                # Save detailed memory profile to file
                with open('memory_profile.log', 'w') as f:
                    for i, mem in enumerate(memory_measurements):
                        f.write(f"{i * 0.1}s: {mem:.2f} MB\n")
                
                logging.info(f"Detailed memory profile saved to 'memory_profile.log'")
            else:
                logging.error("Memory profiling didn't return valid measurements")
                # Fallback to psutil for basic memory info
                process = psutil.Process(os.getpid())
                current_memory = process.memory_info().rss / 1024 / 1024  # MB
                logging.info(f"Current Memory Usage (fallback): {current_memory:.2f} MB")
        
        except Exception as e:
            logging.error(f"Error during memory profiling: {e}")
            logging.info("Falling back to standard execution...")
            run_full_pipeline(use_simulated_data=use_simulated_data)
        
    else:
        # Normal execution path
        use_simulated_data = '--simulate' in sys.argv
        if use_simulated_data:
            logging.info("Running pipeline with simulated large dataset.")
            run_full_pipeline(use_simulated_data=True)
        else:
            logging.info("Running pipeline with original small dataset.")
            run_full_pipeline(use_simulated_data=False)