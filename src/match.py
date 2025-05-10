# src/match.py

from src.database import get_db_connection, close_db_connection
from src.config import FUZZY_MATCH_THRESHOLD
import logging
import psycopg2.extras


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def setup_matching_indexes():
    """
    Creates indexes required for efficient address matching.
    """
    logging.info("Setting up matching indexes...")
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Double Check if the pg_trgm extension is available, install if necessary (requires superuser privileges)
        try:
             cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
             conn.commit()
             logging.info("Ensured pg_trgm extension is available.")
        except psycopg2.Error as e:
             logging.warning(f"Could not create pg_trgm extension (requires superuser): {e}")
             conn.rollback() # Rollback the failed CREATE EXTENSION but continue

        # Create the GIN index on transactions.normalized_address for pg_trgm
        index_query = """
        CREATE INDEX IF NOT EXISTS idx_transactions_normalized_address_gin_trgm
        ON transactions USING GIN (normalized_address gin_trgm_ops);
        """
        cur.execute(index_query)
        conn.commit()
        logging.info("Created GIN index on transactions.normalized_address if it didn't exist.")

        # You might also want an index on canonical_addresses.address (Later added to the schema, but adding this just incase)
        index_canonical_query = """
        CREATE INDEX IF NOT EXISTS idx_canonical_addresses_address_gin_trgm
        ON canonical_addresses USING GIN (address gin_trgm_ops);
        """
        cur.execute(index_canonical_query)
        conn.commit()
        logging.info("Created GIN index on canonical_addresses.address if it didn't exist.")

        # An index for exact matches (Added this directly to the schema)
        index_exact_query_transactions = """
        CREATE INDEX IF NOT EXISTS idx_transactions_normalized_address
        ON transactions (normalized_address);
        """
        cur.execute(index_exact_query_transactions)
        conn.commit()
        logging.info("Created B-tree index on transactions.normalized_address if it didn't exist.")

        index_exact_query_canonical = """
        CREATE INDEX IF NOT EXISTS idx_canonical_addresses_address
        ON canonical_addresses (address);
        """
        cur.execute(index_exact_query_canonical)
        conn.commit()
        logging.info("Created B-tree index on canonical_addresses.address if it didn't exist.")

        # Index on matched_address_id for quickly finding unmatched records
        index_unmatched = """
        CREATE INDEX IF NOT EXISTS idx_transactions_matched_address_id
        ON transactions (matched_address_id);
        """
        cur.execute(index_unmatched)
        conn.commit()
        logging.info("Created index on transactions.matched_address_id if it didn't exist.")


        logging.info("Matching index setup complete.")

    except psycopg2.Error as e:
        logging.error(f"Database error during index setup: {e}")
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred during index setup: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            close_db_connection(conn)


def perform_exact_matching():
    """
    Performs exact matching between transactions and canonical addresses
    based on normalized_address in transactions and address in canonical_addresses.
    Updates the transactions table for successful matches.
    """
    logging.info("Starting exact matching.")

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # First, let's alter the transactions table to add the matching columns if they don't exist
        # This block could potentially be moved to a separate setup function
        try:
            cur.execute("""
            ALTER TABLE transactions
            ADD COLUMN IF NOT EXISTS matched_address_id BIGINT,
            ADD COLUMN IF NOT EXISTS match_type VARCHAR(50),
            ADD COLUMN IF NOT EXISTS confidence_score FLOAT,
            ADD COLUMN IF NOT EXISTS unmatch_reason TEXT,
            ADD COLUMN IF NOT EXISTS parsed_street_number VARCHAR(50),
            ADD COLUMN IF NOT EXISTS parsed_street_name VARCHAR(255),
            ADD COLUMN IF NOT EXISTS parsed_street_suffix VARCHAR(50),
            ADD COLUMN IF NOT EXISTS parsed_unit VARCHAR(50),
            ADD COLUMN IF NOT EXISTS parsed_zip VARCHAR(20);
            """)
            conn.commit()
            logging.info("Added matching columns to transactions table if they didn't exist.")
        except psycopg2.Error as e:
            logging.error(f"Error adding columns to transactions table: {e}")
            conn.rollback()
            raise


        # SQL query for exact matching using direct normalized_address to address comparison
        # Check for existence of normalized_address before this query, although the query WHERE clause handles NULL
        exact_match_query = """
        UPDATE transactions t
        SET
            matched_address_id = ca.address_id,
            match_type = 'exact',
            confidence_score = 1.0, -- 1.0 for exact match
            unmatch_reason = NULL -- Clear unmatch reason on successful match
        FROM canonical_addresses ca
        WHERE
            t.matched_address_id IS NULL AND -- Only process unmatched records
            t.normalized_address IS NOT NULL AND -- Ensure normalized_address exists in transactions
            ca.address IS NOT NULL AND -- Ensure address exists in canonical_addresses
            t.normalized_address = ca.address; -- Direct comparison between normalized addresses
        """

        cur.execute(exact_match_query)
        matched_count = cur.rowcount
        conn.commit()
        logging.info(f"Exact matching completed. Matched {matched_count} records.")

        # Mark records that couldn't be exact matched (and are not already matched)
        unmatch_query = """
        UPDATE transactions t
        SET
            unmatch_reason = 'no exact match found'
        WHERE
            t.matched_address_id IS NULL AND
            t.normalized_address IS NOT NULL AND -- Only consider records that had an address to match
            t.unmatch_reason IS NULL; -- Only mark if not already unmatched for another reason
        """

        cur.execute(unmatch_query)
        marked_unmatched_count = cur.rowcount # This counts records updated in THIS step
        conn.commit()
        logging.info(f"Marked {marked_unmatched_count} records as no exact match found.")


    except psycopg2.Error as e:
        logging.error(f"Database error during exact matching: {e}")
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred during exact matching: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            close_db_connection(conn)

def perform_fuzzy_matching():
    """
    Performs fuzzy matching between transactions and canonical addresses
    based on normalized_address in transactions and address in canonical_addresses.
    Uses address prefix blocking (first 10 characters) for performance.

    Updates the transactions table for successful matches.
    """
    logging.info(f"Starting fuzzy matching (Prefix Blocking Only) with confidence threshold: {FUZZY_MATCH_THRESHOLD}")

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()


        # Get total unmatched transactions count for statistics BEFORE fuzzy matching
        cur.execute("""
            SELECT COUNT(*) FROM transactions
            WHERE matched_address_id IS NULL AND normalized_address IS NOT NULL
        """)
        unmatched_count_before = cur.fetchone()[0]
        logging.info(f"Starting fuzzy matching with {unmatched_count_before} unmatched transactions")

        # --- BLOCKING STRATEGY 1: Address Prefix (First N characters, using 10 as in original) ---
        # Uses pg_trgm extension and GIN indexes on normalized_address and address.
        # The confidence score calculation (average of similarity and word_similarity) is specific
        # to pg_trgm functions.
        fuzzy_match_prefix_query = """
        WITH fuzzy_matches AS (
            SELECT
                t.id,
                ca.address_id,
                -- Calculate confidence score as an average of similarity and word_similarity
                (similarity(t.normalized_address, ca.address) +
                 word_similarity(t.normalized_address, ca.address)) / 2.0 AS confidence,
                -- Get rank for best match within the block
                ROW_NUMBER() OVER (PARTITION BY t.id ORDER BY ((similarity(t.normalized_address, ca.address) + word_similarity(t.normalized_address, ca.address)) / 2.0) DESC) as rank
            FROM
                transactions t
            JOIN canonical_addresses ca ON
                -- Block by the first 10 characters
                LEFT(t.normalized_address, 10) = LEFT(ca.address, 10)
            WHERE
                t.matched_address_id IS NULL AND -- Only match records not already matched
                t.normalized_address IS NOT NULL AND -- Transaction must have a normalized address
                ca.address IS NOT NULL AND -- Canonical address must exist
                -- Only consider matches above a minimum similarity threshold to reduce candidates
                similarity(t.normalized_address, ca.address) > %s
        )
        UPDATE transactions t
        SET
            matched_address_id = fm.address_id,
            match_type = 'fuzzy_prefix_block',
            confidence_score = fm.confidence,
            unmatch_reason = NULL -- Clear reason if a match is found
        FROM
            fuzzy_matches fm
        WHERE
            t.id = fm.id AND -- Join back to the transaction
            fm.rank = 1 AND -- Only use the top match within the block
            fm.confidence >= %s; -- Apply the confidence threshold for the final match decision
        """

        # Execute the prefix blocking fuzzy matching query
        # Pass the threshold parameter twice for the two %s placeholders
        cur.execute(fuzzy_match_prefix_query, (FUZZY_MATCH_THRESHOLD, FUZZY_MATCH_THRESHOLD))
        matched_prefix_count = cur.rowcount
        conn.commit()
        logging.info(f"Prefix blocking fuzzy matching completed. Matched {matched_prefix_count} records.")


        # Mark records that couldn't be matched after the prefix attempt
        unmatch_query = """
        UPDATE transactions t
        SET
            unmatch_reason = 'no fuzzy match found with sufficient confidence (prefix block)'
        WHERE
            t.matched_address_id IS NULL AND
            t.normalized_address IS NOT NULL AND -- Only mark if it had an address to process
            t.unmatch_reason IS NULL; -- Only mark if not already unmatched
        """

        cur.execute(unmatch_query)
        marked_unmatched_count_fuzzy = cur.rowcount # Records marked as unmatched in this step
        conn.commit()

        # To get the total remaining unmatched count after fuzzy matching, you'd query again
        cur.execute("""
            SELECT COUNT(*) FROM transactions
            WHERE matched_address_id IS NULL AND normalized_address IS NOT NULL
        """)
        remaining_unmatched_count = cur.fetchone()[0]


        # Log overall statistics
        logging.info(f"Fuzzy matching summary (Prefix Blocking Only): {matched_prefix_count} matched in this step. "
                     f"{marked_unmatched_count_fuzzy} records newly marked as no fuzzy match found with sufficient confidence. "
                     f"Total remaining unmatched with normalized address: {remaining_unmatched_count}")


    except psycopg2.Error as e:
        logging.error(f"Database error during fuzzy matching: {e}")
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred during fuzzy matching: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            close_db_connection(conn)


if __name__ == "__main__":
    # Example usage:
    # Ensure data is ingested and parsed before running this.
    try:
        logging.info("Starting matching script.")

        # --- Setup indexes before matching ---
        setup_matching_indexes()

        # --- Perform matching strategies in order ---
        perform_exact_matching()

        logging.info("Matching script finished.")

    except Exception as e:
        logging.error(f"Matching script failed: {e}")