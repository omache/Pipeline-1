import logging
import psycopg2
import psycopg2.extras
import jellyfish
from rapidfuzz import fuzz
from functools import lru_cache

# Assuming these are in src.database and src.config as in match.py
from src.database import get_db_connection, close_db_connection
from src.config import PHONETIC_MATCH_CONFIDENCE, PHONETIC_TIEBREAK_THRESHOLD

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Adding caching for phonetic functions to avoid recalculating the same values
@lru_cache(maxsize=10000)
def cached_metaphone(text):
    return jellyfish.metaphone(text) if text else None

@lru_cache(maxsize=10000)
def cached_soundex(text):
    return jellyfish.soundex(text) if text else None

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
        """)
        conn.commit()
        logging.info("Phonetic key columns ensured.")

        # Create indexes on phonetic columns if they don't exist
        cur.execute("""
            SELECT indexname FROM pg_indexes 
            WHERE tablename = 'canonical_addresses' AND indexname = 'idx_canonical_metaphone';
        """)
        if not cur.fetchone():
            logging.info("Creating index on metaphone_key column...")
            cur.execute("CREATE INDEX idx_canonical_metaphone ON canonical_addresses (metaphone_key);")
            conn.commit()
            
        cur.execute("""
            SELECT indexname FROM pg_indexes 
            WHERE tablename = 'canonical_addresses' AND indexname = 'idx_canonical_soundex';
        """)
        if not cur.fetchone():
            logging.info("Creating index on soundex_key column...")
            cur.execute("CREATE INDEX idx_canonical_soundex ON canonical_addresses (soundex_key);")
            conn.commit()

        batch_size = 5000
        
        if force_recompute_all:
            cur.execute("SELECT COUNT(*) FROM canonical_addresses WHERE address IS NOT NULL")
            total_count = cur.fetchone()[0]
            logging.info(f"Force recompute: processing {total_count} non-NULL canonical addresses.")
            
            for offset in range(0, total_count, batch_size):
                logging.info(f"Processing batch {offset//batch_size + 1}, addresses {offset} to {min(offset+batch_size, total_count)}")
                cur.execute("""
                    SELECT address_id, address FROM canonical_addresses
                    WHERE address IS NOT NULL
                    ORDER BY address_id
                    LIMIT %s OFFSET %s
                """, (batch_size, offset))
                process_phonetic_batch(conn, cur)
        else:
            cur.execute("SELECT COUNT(*) FROM canonical_addresses WHERE address IS NOT NULL AND (metaphone_key IS NULL OR soundex_key IS NULL)")
            total_count = cur.fetchone()[0]
            logging.info(f"Processing {total_count} canonical addresses with missing phonetic keys.")
            
            for offset in range(0, total_count, batch_size):
                logging.info(f"Processing batch {offset//batch_size + 1}, addresses {offset} to {min(offset+batch_size, total_count)}")
                cur.execute("""
                    SELECT address_id, address FROM canonical_addresses
                    WHERE address IS NOT NULL AND (metaphone_key IS NULL OR soundex_key IS NULL)
                    ORDER BY address_id
                    LIMIT %s OFFSET %s
                """, (batch_size, offset))
                process_phonetic_batch(conn, cur)

    except psycopg2.Error as e:
        logging.error(f"Database error during preparation of phonetic keys: {e}")
        if conn:
            conn.rollback()
        raise
    except TypeError as e:
        logging.error(f"Type error during phonetic key generation: {e}")
        raise
    finally:
        if cur:
            cur.close()

def process_phonetic_batch(conn, cur):
    """Process a batch of addresses to compute their phonetic keys"""
    canonical_addresses_to_update = cur.fetchall()
    
    if not canonical_addresses_to_update:
        return
        
    updates = []
    for addr_id, address_text in canonical_addresses_to_update:
        # Use cached functions
        metaphone_k = cached_metaphone(address_text)
        soundex_k = cached_soundex(address_text)
        updates.append((metaphone_k, soundex_k, addr_id))

    if updates:
        psycopg2.extras.execute_batch(cur, """
            UPDATE canonical_addresses
            SET metaphone_key = %s, soundex_key = %s
            WHERE address_id = %s
        """, updates, page_size=1000)  # Optimized page_size for batch processing
        conn.commit()
        logging.info(f"Updated phonetic keys for {len(updates)} canonical addresses.")

def perform_fallback_matching():
    """
    Performs phonetic (Metaphone/Soundex) matching as a fallback strategy
    for transactions not matched by previous methods.
    """
    logging.info("Starting fallback matching (phonetic).")
    conn = None
    updated_count = 0
    batch_size = 50000  # Process in batches for better memory usage

    try:
        conn = get_db_connection()
        
        # Use a named cursor for server-side cursors to handle large datasets
        with conn.cursor(name='large_cursor') as counter_cur:
            counter_cur.execute("""
                SELECT COUNT(*)
                FROM transactions
                WHERE matched_address_id IS NULL
                  AND normalized_address IS NOT NULL AND normalized_address != ''
                  AND (unmatch_reason = 'no fuzzy match found with sufficient confidence' OR
                       unmatch_reason = 'no exact match found' OR 
                       unmatch_reason IS NULL);
            """)
            total_transactions = counter_cur.fetchone()[0]
        
        logging.info(f"Found {total_transactions} transactions for fallback (phonetic) matching.")
        
        if total_transactions == 0:
            logging.info("No transactions to process for fallback (phonetic) matching.")
            return
        
        # Preparation step: Ensure phonetic keys are available
        logging.info("Preparing phonetic keys in canonical_addresses for the matching session.")
        ensure_phonetic_columns_and_keys(conn, force_recompute_all=False)
        logging.info("Phonetic key preparation complete.")
        
        # Load all canonical addresses with phonetic keys into memory once
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT address_id, address, metaphone_key, soundex_key
                FROM canonical_addresses
                WHERE address IS NOT NULL AND (metaphone_key IS NOT NULL OR soundex_key IS NOT NULL);
            """)
            all_canonical = cur.fetchall()
            
            if not all_canonical:
                logging.warning("No canonical addresses with phonetic keys found. Fallback (phonetic) matching will not be effective.")
                return
                
            # Build lookup dictionaries for fast phonetic matching
            canonical_by_metaphone = {}
            canonical_by_soundex = {}
            for ca_row in all_canonical:
                if ca_row['metaphone_key']:
                    canonical_by_metaphone.setdefault(ca_row['metaphone_key'], []).append(ca_row)
                if ca_row['soundex_key']:
                    canonical_by_soundex.setdefault(ca_row['soundex_key'], []).append(ca_row)
            logging.info(f"Loaded {len(all_canonical)} canonical addresses into phonetic lookup dictionaries.")
            
            # Process transactions in batches
            for offset in range(0, total_transactions, batch_size):
                logging.info(f"Processing transaction batch {offset//batch_size + 1}, transactions {offset} to {min(offset+batch_size, total_transactions)}")
                
                # Use a regular cursor for each batch
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as batch_cur:
                    batch_cur.execute("""
                        SELECT id, normalized_address
                        FROM transactions
                        WHERE matched_address_id IS NULL
                          AND normalized_address IS NOT NULL AND normalized_address != ''
                          AND (unmatch_reason = 'no fuzzy match found with sufficient confidence' OR
                               unmatch_reason = 'no exact match found' OR 
                               unmatch_reason IS NULL)
                        ORDER BY id
                        LIMIT %s OFFSET %s;
                    """, (batch_size, offset))
                    
                    unmatched_transactions = batch_cur.fetchall()
                    processed_transaction_ids = [t['id'] for t in unmatched_transactions]
                    
                    if not unmatched_transactions:
                        continue
                        
                    # Process the batch
                    match_updates = []
                    for trans in unmatched_transactions:
                        # Use cached functions for performance
                        trans_metaphone = cached_metaphone(trans['normalized_address'])
                        trans_soundex = cached_soundex(trans['normalized_address'])
                        
                        potential_matches = {}
                        
                        # Look up by metaphone
                        if trans_metaphone and trans_metaphone in canonical_by_metaphone:
                            for ca in canonical_by_metaphone[trans_metaphone]:
                                potential_matches[ca['address_id']] = ca
                        
                        # Look up by soundex
                        if trans_soundex and trans_soundex in canonical_by_soundex:
                            for ca in canonical_by_soundex[trans_soundex]:
                                potential_matches[ca['address_id']] = ca
                        
                        best_match_ca_id = None
                        best_tiebreak_score = -1
                        
                        # Find the best match among potential matches
                        if potential_matches:
                            for ca_id, ca_data in potential_matches.items():
                                # Only calculate ratio for potential matches that passed phonetic filter
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
                    
                    # Apply matches in bulk
                    if match_updates:
                        update_query = """
                            UPDATE transactions
                            SET matched_address_id = %s,
                                match_type = %s,
                                confidence_score = %s,
                                unmatch_reason = %s
                            WHERE id = %s;
                        """
                        psycopg2.extras.execute_batch(batch_cur, update_query, match_updates, page_size=1000)
                        conn.commit()
                        logging.info(f"Fallback (phonetic) matching updated {len(match_updates)} records in current batch.")
                    
                    # Mark remaining as unmatched
                    if processed_transaction_ids:
                        # Use IN clause with array for better performance with large lists
                        final_unmatch_query = """
                        UPDATE transactions
                        SET unmatch_reason = 'no phonetic match found'
                        WHERE id = ANY(%s) AND matched_address_id IS NULL;
                        """
                        batch_cur.execute(final_unmatch_query, (processed_transaction_ids,))
                        conn.commit()
                        logging.info(f"Marked {batch_cur.rowcount} processed records as 'no phonetic match found' in current batch.")

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
        if conn:
            close_db_connection(conn)
        logging.info(f"Fallback (phonetic) matching finished. Matched {updated_count} records total.")

if __name__ == "__main__":
    logging.info("Starting fallback (phonetic) matching script directly.")
    try:
        perform_fallback_matching()
        logging.info("Fallback (phonetic) matching script finished successfully.")
    except Exception as e:
        logging.error(f"Fallback (phonetic) matching script failed when run directly: {e}")