# src/fallback.py

import logging
import psycopg2
import psycopg2.extras
import jellyfish
from rapidfuzz import fuzz  # For tie-breaking

# Assuming these are in src.database and src.config as in match.py
from src.database import get_db_connection, close_db_connection
from src.config import PHONETIC_MATCH_CONFIDENCE, PHONETIC_TIEBREAK_THRESHOLD

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def ensure_phonetic_columns_and_keys(conn, force_recompute_all=False):
    """
    Ensures canonical_addresses table has phonetic key columns and populates them.
    Set force_recompute_all=True to re-calculate for all addresses, otherwise only for NULL keys.
    """
    cur = None
    try:
        cur = conn.cursor()
        logging.info("Ensuring phonetic key columns (metaphone_key, soundex_key) exist in canonical_addresses.")
        cur.execute("""
            ALTER TABLE canonical_addresses
            ADD COLUMN IF NOT EXISTS metaphone_key VARCHAR(255),
            ADD COLUMN IF NOT EXISTS soundex_key VARCHAR(4);
        """)  # Soundex is typically 4 chars, Metaphone can be longer
        conn.commit()
        logging.info("Phonetic key columns ensured.")

        logging.info("Populating phonetic keys for canonical_addresses. This might take some time...")
        if force_recompute_all:
            cur.execute("SELECT address_id, address FROM canonical_addresses WHERE address IS NOT NULL") # Ensure address is not null
            logging.info("Force recompute: processing all non-NULL canonical addresses.")
        else:
            cur.execute("""
                SELECT address_id, address FROM canonical_addresses
                WHERE address IS NOT NULL AND (metaphone_key IS NULL OR soundex_key IS NULL)
            """)
            logging.info("Processing canonical addresses with missing phonetic keys for non-NULL addresses.")

        canonical_addresses_to_update = cur.fetchall()
        
        if not canonical_addresses_to_update:
            logging.info("No canonical addresses found needing phonetic key updates based on criteria.")
            return

        logging.info(f"Found {len(canonical_addresses_to_update)} canonical addresses to update/generate phonetic keys.")
        updates = []
        for addr_id, address_text in canonical_addresses_to_update:
            # jellyfish functions handle None by raising TypeError, address IS NOT NULL in SQL helps.
            # Empty strings will result in empty phonetic keys by jellyfish.
            metaphone_k = jellyfish.metaphone(address_text)
            soundex_k = jellyfish.soundex(address_text)
            updates.append((metaphone_k, soundex_k, addr_id))

        if updates:
            psycopg2.extras.execute_batch(cur, """
                UPDATE canonical_addresses
                SET metaphone_key = %s, soundex_key = %s
                WHERE address_id = %s
            """, updates)
            conn.commit()
            logging.info(f"Updated phonetic keys for {len(updates)} canonical addresses.")
        else:
            logging.info("No valid updates generated for phonetic keys (perhaps all addresses were NULL or empty initially selected).")

    except psycopg2.Error as e:
        logging.error(f"Database error during preparation of phonetic keys: {e}")
        if conn:
            conn.rollback()
        raise
    except TypeError as e:
        logging.error(f"Type error during phonetic key generation (likely None address was passed despite SQL check): {e}")
        raise
    finally:
        if cur:
            cur.close()

def perform_fallback_matching(): # RENAMED FUNCTION HERE
    """
    Performs phonetic (Metaphone/Soundex) matching as a fallback strategy
    for transactions not matched by previous methods.
    """
    logging.info("Starting fallback matching (phonetic).")
    conn = None
    updated_count = 0
    processed_transaction_ids = []

    try:
        conn = get_db_connection()
        
        # Preparation step: Ensure phonetic keys are available.
        # Consider running this as a separate, less frequent maintenance task if canonical_addresses is large.
        # For now, it's called here to ensure functionality.
        logging.info("Preparing phonetic keys in canonical_addresses for the matching session.")
        ensure_phonetic_columns_and_keys(conn, force_recompute_all=False)
        logging.info("Phonetic key preparation complete.")

        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cur.execute("""
            SELECT id, normalized_address
            FROM transactions
            WHERE matched_address_id IS NULL
              AND normalized_address IS NOT NULL AND normalized_address != ''
              AND (unmatch_reason = 'no fuzzy match found with sufficient confidence' OR
                   unmatch_reason = 'no exact match found' OR 
                   unmatch_reason IS NULL); 
        """)
        unmatched_transactions = cur.fetchall()
        logging.info(f"Found {len(unmatched_transactions)} transactions for fallback (phonetic) matching.")

        if not unmatched_transactions:
            logging.info("No transactions to process for fallback (phonetic) matching.")
            return

        processed_transaction_ids = [t['id'] for t in unmatched_transactions]

        cur.execute("""
            SELECT address_id, address, metaphone_key, soundex_key
            FROM canonical_addresses
            WHERE address IS NOT NULL AND (metaphone_key IS NOT NULL OR soundex_key IS NOT NULL);
        """)
        all_canonical = cur.fetchall()

        if not all_canonical:
            logging.warning("No canonical addresses with phonetic keys found. Fallback (phonetic) matching will not be effective.")
            # Mark remaining as unmatchable if this is a critical failure
            if processed_transaction_ids:
                ids_for_sql = tuple(processed_transaction_ids)
                final_unmatch_query = """
                UPDATE transactions
                SET unmatch_reason = 'no phonetic match (no canonical keys)'
                WHERE id IN %s AND matched_address_id IS NULL;
                """
                cur.execute(final_unmatch_query, (ids_for_sql,))
                conn.commit()
                logging.info(f"Marked {cur.rowcount} processed records as 'no phonetic match (no canonical keys)'.")
            return

        canonical_by_metaphone = {}
        canonical_by_soundex = {}
        for ca_row in all_canonical:
            if ca_row['metaphone_key']: # Ensure key is not empty if that's possible from jellyfish
                canonical_by_metaphone.setdefault(ca_row['metaphone_key'], []).append(ca_row)
            if ca_row['soundex_key']: # Ensure key is not empty
                canonical_by_soundex.setdefault(ca_row['soundex_key'], []).append(ca_row)
        logging.info(f"Loaded {len(all_canonical)} canonical addresses into phonetic lookup dictionaries.")

        match_updates = []
        for trans in unmatched_transactions:
            # normalized_address is already checked for NULL/empty in the initial SQL query
            trans_metaphone = jellyfish.metaphone(trans['normalized_address'])
            trans_soundex = jellyfish.soundex(trans['normalized_address'])
            
            potential_matches = {} 

            if trans_metaphone and trans_metaphone in canonical_by_metaphone:
                for ca in canonical_by_metaphone[trans_metaphone]:
                    potential_matches[ca['address_id']] = ca
            
            if trans_soundex and trans_soundex in canonical_by_soundex:
                for ca in canonical_by_soundex[trans_soundex]:
                    potential_matches[ca['address_id']] = ca
            
            best_match_ca_id = None
            best_tiebreak_score = -1 

            if potential_matches:
                for ca_id, ca_data in potential_matches.items():
                    score = fuzz.ratio(trans['normalized_address'], ca_data['address'])
                    if score > best_tiebreak_score and score >= PHONETIC_TIEBREAK_THRESHOLD:
                        best_tiebreak_score = score
                        best_match_ca_id = ca_data['address_id']
                
                if best_match_ca_id:
                    match_updates.append((
                        best_match_ca_id,
                        'phonetic', 
                        PHONETIC_MATCH_CONFIDENCE, 
                        None, 
                        trans['id']
                    ))
                    updated_count += 1
        
        if match_updates:
            update_query = """
                UPDATE transactions
                SET matched_address_id = %s,
                    match_type = %s,
                    confidence_score = %s,
                    unmatch_reason = %s
                WHERE id = %s;
            """
            psycopg2.extras.execute_batch(cur, update_query, match_updates)
            conn.commit()
            logging.info(f"Fallback (phonetic) matching updated {len(match_updates)} records.")

        if processed_transaction_ids:
            ids_for_sql = tuple(processed_transaction_ids)
            final_unmatch_query = """
            UPDATE transactions
            SET unmatch_reason = 'no phonetic match found'
            WHERE id IN %s AND matched_address_id IS NULL;
            """
            cur.execute(final_unmatch_query, (ids_for_sql,))
            unmatched_after_phonetic = cur.rowcount
            conn.commit()
            logging.info(f"Marked {unmatched_after_phonetic} processed records as 'no phonetic match found' after fallback attempts.")

    except psycopg2.Error as e:
        logging.error(f"Database error during fallback (phonetic) matching: {e}")
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred during fallback (phonetic) matching: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cur: # Ensure cursor is closed if it was opened
            cur.close()
        if conn:
            close_db_connection(conn)
        logging.info(f"Fallback (phonetic) matching finished. Matched {updated_count} records.")

if __name__ == "__main__":
    logging.info("Starting fallback (phonetic) matching script directly.")
    try:
        perform_fallback_matching() # Call the RENAMED function
        logging.info("Fallback (phonetic) matching script finished successfully.")
    except Exception as e:
        logging.error(f"Fallback (phonetic) matching script failed when run directly: {e}")