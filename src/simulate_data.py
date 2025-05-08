# src/simulate_data.py
# Script to simulate a large transaction dataset by duplicating existing data
# using a memory-efficient chunked writing approach.

import pandas as pd
import os
import logging
# Assuming TRANSACTIONS_CSV, SIMULATED_TRANSACTIONS_CSV are defined in src.config
# OUTPUT_DIR is implicitly part of SIMULATED_TRANSACTIONS_CSV path
from src.config import TRANSACTIONS_CSV, SIMULATED_TRANSACTIONS_CSV

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def simulate_large_transactions_csv(target_rows=2000, input_csv_path=TRANSACTIONS_CSV, output_csv_path=SIMULATED_TRANSACTIONS_CSV):
    logging.info(f"Starting data simulation (memory-efficient chunked writing) to reach approximately {target_rows} rows.")

    try:
        if not os.path.exists(input_csv_path):
            logging.error(f"Error: Original transactions CSV not found at {input_csv_path}")
            raise FileNotFoundError(f"Input CSV not found: {input_csv_path}")

        df_small = pd.read_csv(input_csv_path)
        original_rows = len(df_small)
        logging.info(f"Read {original_rows} rows from original CSV: {input_csv_path}")

        if original_rows == 0:
            logging.error("Original CSV is empty. Cannot simulate data.")
            return

        if 'id' not in df_small.columns:
            logging.error(f"'id' column not found in {input_csv_path}. Unique IDs cannot be generated as intended.")
            raise ValueError(f"'id' column missing in {input_csv_path}. This is required for the current simulation logic.")

        output_dir = os.path.dirname(output_csv_path)
        if output_dir and not os.path.exists(output_dir): # Check if output_dir is not an empty string
            os.makedirs(output_dir)
            logging.info(f"Created output directory: {output_dir}")

        if original_rows >= target_rows:
            logging.warning(f"Original CSV ({input_csv_path}) already has {original_rows} rows, which is >= target {target_rows}. Copying original file to {output_csv_path}.")
            df_small.to_csv(output_csv_path, index=False, mode='w') # mode='w' to overwrite
            logging.info(f"Copied original CSV to {output_csv_path}")
            return

        # Write header once, overwriting the file if it exists
        df_small.head(0).to_csv(output_csv_path, index=False, mode='w')

        rows_written_total = 0
        duplication_idx = 0
        
        logging.info(f"Simulating data and writing in chunks to {output_csv_path}...")
        while rows_written_total < target_rows:
            temp_df = df_small.copy()
            
            temp_df['id'] = temp_df['id'].astype(str) + f"_{duplication_idx}"
            
            rows_in_current_chunk = len(temp_df)
            
            if rows_written_total + rows_in_current_chunk > target_rows:
                rows_needed = target_rows - rows_written_total
                temp_df = temp_df.head(rows_needed) # Trim if this chunk overshoots
            
            if len(temp_df) == 0: 
                break

            temp_df.to_csv(output_csv_path, index=False, mode='a', header=False)
            rows_written_total += len(temp_df)
            duplication_idx += 1

            # Log progress periodically to avoid flooding logs but show activity
            if duplication_idx % 1000 == 0 or (original_rows > 0 and rows_written_total % (original_rows * 100) == 0 and rows_written_total > 0):
                 logging.info(f"Rows written: {rows_written_total} / {target_rows} (Duplication cycle: {duplication_idx})")
        
        logging.info(f"Simulated data saved successfully to {output_csv_path} with {rows_written_total} rows.")

    except FileNotFoundError: 
        logging.error(f"Error: File not found during simulation. Input: {input_csv_path}")
        raise
    except ValueError as ve: # Catch the ValueError raised for missing id
        logging.error(f"Configuration error for simulation: {ve}")
        raise # Re-raise to halt pipeline if this is critical
    except Exception as e:
        logging.error(f"An unexpected error occurred during data simulation: {e}")
        raise

if __name__ == "__main__":
    try:
        simulate_large_transactions_csv() 
        logging.info("Data simulation script finished.")
    except Exception as e:
        logging.error(f"Data simulation script failed: {e}")