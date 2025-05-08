# src/match.py

# Module for performing different address matching strategies.

from src.database import get_db_connection, close_db_connection
from src.config import FUZZY_MATCH_THRESHOLD # This line imports the threshold
import logging
import psycopg2.extras
from rapidfuzz import fuzz # Using RapidFuzz for potentially better performance
import jellyfish # For phonetic matching

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

        # Check if normalized_address column exists in transactions table
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'transactions' AND column_name = 'normalized_address';
        """)
        normalized_address_exists = cur.fetchone() is not None

        if not normalized_address_exists:
            logging.error("normalized_address column does not exist in transactions table")
            raise Exception("normalized_address column required for exact matching")

        # SQL query for exact matching using direct normalized_address to address comparison
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
            t.normalized_address IS NOT NULL AND -- Ensure normalized_address exists
            t.normalized_address = ca.address; -- Direct comparison between normalized addresses
        """

        cur.execute(exact_match_query)
        matched_count = cur.rowcount
        conn.commit()
        logging.info(f"Exact matching completed. Matched {matched_count} records.")

        # Mark records that couldn't be matched
        unmatch_query = """
        UPDATE transactions t
        SET
            unmatch_reason = 'no exact match found'
        WHERE
            t.matched_address_id IS NULL AND
            t.normalized_address IS NOT NULL AND
            t.unmatch_reason IS NULL;
        """

        cur.execute(unmatch_query)
        unmatched_count = cur.rowcount
        conn.commit()
        logging.info(f"Marked {unmatched_count} records as unmatched.")

        # Log overall statistics
        logging.info(f"Exact matching summary: {matched_count} matched, {unmatched_count} explicitly unmatched.")

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
    logging.info(f"Starting fuzzy matching (Prefix Blocking Only) with threshold: {FUZZY_MATCH_THRESHOLD}")

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Check if normalized_address column exists in transactions table
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'transactions' AND column_name = 'normalized_address';
        """)
        normalized_address_exists = cur.fetchone() is not None

        if not normalized_address_exists:
            logging.error("normalized_address column does not exist in transactions table")
            raise Exception("normalized_address column required for fuzzy matching")

        # Get total unmatched transactions count for statistics
        cur.execute("""
            SELECT COUNT(*) FROM transactions
            WHERE matched_address_id IS NULL AND normalized_address IS NOT NULL
        """)
        unmatched_count_before = cur.fetchone()[0]
        logging.info(f"Starting fuzzy matching with {unmatched_count_before} unmatched transactions")

        # --- BLOCKING STRATEGY 1: Address Prefix (First 10 characters) ---
        fuzzy_match_prefix_query = """
        WITH fuzzy_matches AS (
            SELECT
                t.id,
                ca.address_id,
                similarity(t.normalized_address, ca.address) AS sim_score,
                -- Using both similarity and trigram similarity for better matching
                (similarity(t.normalized_address, ca.address) +
                 word_similarity(t.normalized_address, ca.address)) / 2.0 AS confidence
            FROM
                transactions t
            JOIN canonical_addresses ca ON
                LEFT(t.normalized_address, 10) = LEFT(ca.address, 10)
            WHERE
                t.matched_address_id IS NULL AND
                t.normalized_address IS NOT NULL AND
                -- Only consider matches with reasonable similarity
                similarity(t.normalized_address, ca.address) > %s
        ),
        best_matches AS (
            SELECT
                id,
                address_id,
                confidence
            FROM (
                SELECT
                    id,
                    address_id,
                    confidence,
                    ROW_NUMBER() OVER (PARTITION BY id ORDER BY confidence DESC) as rank
                FROM
                    fuzzy_matches
                WHERE
                    -- Only accept confident matches
                    confidence > %s
            ) ranked
            WHERE rank = 1 -- Take only the best match for each transaction
        )
        UPDATE transactions t
        SET
            matched_address_id = bm.address_id,
            match_type = 'fuzzy_prefix_block',
            confidence_score = bm.confidence,
            unmatch_reason = NULL
        FROM
            best_matches bm
        WHERE
            t.id = bm.id;
        """

        # Execute the prefix blocking fuzzy matching query
        cur.execute(fuzzy_match_prefix_query, (FUZZY_MATCH_THRESHOLD, FUZZY_MATCH_THRESHOLD))
        matched_prefix_count = cur.rowcount
        conn.commit()
        logging.info(f"Prefix blocking fuzzy matching completed. Matched {matched_prefix_count} records.")

        # --- Removed: Blocking Strategy 2 (Street Name) and Fallback Strategy ---

        # Mark records that couldn't be matched after the prefix attempt
        unmatch_query = """
        UPDATE transactions t
        SET
            unmatch_reason = 'no fuzzy match found with sufficient confidence (prefix block)'
        WHERE
            t.matched_address_id IS NULL AND
            t.normalized_address IS NOT NULL AND
            t.unmatch_reason IS NULL;
        """

        cur.execute(unmatch_query)
        still_unmatched_count = cur.rowcount
        conn.commit()

        # Log overall statistics
        logging.info(f"Fuzzy matching summary (Prefix Blocking Only): {matched_prefix_count} matched, "
                     f"{still_unmatched_count} explicitly unmatched after prefix block.")


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
        perform_exact_matching()
        perform_fuzzy_matching()
        logging.info("Matching script finished.")
    except Exception as e:
        logging.error(f"Matching script failed: {e}")