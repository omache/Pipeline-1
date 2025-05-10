# src/api.py

import logging
import os
import sys
from flask import Flask, request, jsonify
import psycopg2
import psycopg2.extras
import jellyfish
from rapidfuzz import fuzz

if __name__ == "__main__":
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))

from src.database import get_db_connection, close_db_connection
from src.parse import parse_and_normalize_address # Import the parsing
from src.config import DB_CONFIG, FUZZY_MATCH_THRESHOLD, PHONETIC_MATCH_CONFIDENCE, PHONETIC_TIEBREAK_THRESHOLD

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

# Pre-calculate phonetic keys for canonical addresses if not already done
# This function is adapted from fallback_match.py to run during API startup or first request
def ensure_canonical_phonetic_keys():
    """Ensures canonical_addresses table has phonetic key columns and populates them."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        logging.info("API startup: Checking/Ensuring phonetic key columns and populating canonical addresses.")

        # Add columns if they don't exist
        cur.execute("""
            ALTER TABLE canonical_addresses
            ADD COLUMN IF NOT EXISTS metaphone_key VARCHAR(255),
            ADD COLUMN IF NOT EXISTS soundex_key VARCHAR(4);
        """)
        conn.commit()

        # Populate missing phonetic keys in batches
        batch_size = 5000
        offset = 0
        while True:
            cur.execute("""
                SELECT address_id, address FROM canonical_addresses
                WHERE address IS NOT NULL AND (metaphone_key IS NULL OR soundex_key IS NULL)
                ORDER BY address_id
                LIMIT %s OFFSET %s
            """, (batch_size, offset))
            batch = cur.fetchall()

            if not batch:
                break

            updates = []
            for addr_id, address_text in batch:
                metaphone_k = jellyfish.metaphone(address_text) if address_text else None
                soundex_k = jellyfish.soundex(address_text) if address_text else None
                updates.append((metaphone_k, soundex_k, addr_id))

            if updates:
                psycopg2.extras.execute_batch(cur, """
                    UPDATE canonical_addresses
                    SET metaphone_key = %s, soundex_key = %s
                    WHERE address_id = %s
                """, updates, page_size=1000)
                conn.commit()
                logging.info(f"Populated phonetic keys for {len(updates)} canonical addresses.")

            offset += len(batch)

        logging.info("Phonetic key preparation complete.")

    except psycopg2.Error as e:
        logging.error(f"Database error during phonetic key preparation: {e}")
        if conn:
            conn.rollback()
        # Allow the API to start, but matching might fail if keys are missing
    except Exception as e:
        logging.error(f"An unexpected error occurred during phonetic key preparation: {e}")
        # Allow the API to start, but matching might fail if keys are missing
    finally:
        if conn:
            close_db_connection(conn)


# --- Matching Logic (Adapted for single address lookup) ---

def find_best_match(conn, normalized_address):
    """
    Finds the best match for a single normalized address in canonical_addresses.
    Applies exact, fuzzy (prefix block), and phonetic matching in order.
    """
    if not normalized_address:
        return None

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    best_match = None
    best_confidence = -1.0
    match_type = None

    try:
        # 1. Exact Match
        logging.debug(f"Attempting exact match for: {normalized_address}")
        cur.execute("""
            SELECT address_id, address
            FROM canonical_addresses
            WHERE address = %s
            LIMIT 1;
        """, (normalized_address,))
        exact_match = cur.fetchone()

        if exact_match:
            logging.debug("Exact match found.")
            return {
                'address_id': exact_match['address_id'],
                'canonical_address': exact_match['address'],
                'match_type': 'exact',
                'confidence_score': 1.0
            }

        # 2. Fuzzy Match (Prefix Blocking)
        logging.debug(f"No exact match. Attempting fuzzy match for: {normalized_address}")
        prefix = normalized_address[:10] # Use the same prefix block as in match.py
        if prefix:
            cur.execute("""
                SELECT address_id, address
                FROM canonical_addresses
                WHERE LEFT(address, 10) = %s
                AND similarity(address, %s) > %s; -- Preliminary similarity check using pg_trgm
            """, (prefix, normalized_address, FUZZY_MATCH_THRESHOLD * 0.8)) # Use a slightly lower threshold for initial candidates

            potential_fuzzy_matches = cur.fetchall()

            if potential_fuzzy_matches:
                logging.debug(f"Found {len(potential_fuzzy_matches)} fuzzy candidates in prefix block.")
                best_fuzzy_score = -1.0
                best_fuzzy_match_id = None
                best_fuzzy_canonical = None

                for ca in potential_fuzzy_matches:
                    # Calculate combined similarity score using pg_trgm functions after fetching candidates
                    # This leverages the GIN index for candidate retrieval first.
                    cur.execute("SELECT (similarity(%s, %s) + word_similarity(%s, %s)) / 2.0 AS score",
                                (normalized_address, ca['address'], normalized_address, ca['address']))
                    score = cur.fetchone()['score']


                    if score is not None and score > best_fuzzy_score and score >= FUZZY_MATCH_THRESHOLD:
                        best_fuzzy_score = score
                        best_fuzzy_match_id = ca['address_id']
                        best_fuzzy_canonical = ca['address']

                if best_fuzzy_match_id:
                     logging.debug(f"Best fuzzy match found with score {best_fuzzy_score:.2f}")
                     return {
                         'address_id': best_fuzzy_match_id,
                         'canonical_address': best_fuzzy_canonical,
                         'match_type': 'fuzzy_prefix_block',
                         'confidence_score': round(best_fuzzy_score, 2)
                     }

        # 3. Phonetic Fallback Match
        logging.debug(f"No fuzzy match. Attempting phonetic fallback for: {normalized_address}")
        trans_metaphone = jellyfish.metaphone(normalized_address) if normalized_address else None
        trans_soundex = jellyfish.soundex(normalized_address) if normalized_address else None


        if trans_metaphone or trans_soundex:
            # Query canonical addresses by phonetic keys
            # Use OR condition to broaden the search
            cur.execute("""
                SELECT address_id, address
                FROM canonical_addresses
                WHERE (metaphone_key = %s AND metaphone_key IS NOT NULL)
                OR (soundex_key = %s AND soundex_key IS NOT NULL);
            """, (trans_metaphone, trans_soundex))

            potential_phonetic_matches = cur.fetchall()

            if potential_phonetic_matches:
                logging.debug(f"Found {len(potential_phonetic_matches)} phonetic candidates.")
                best_phonetic_score = -1.0
                best_phonetic_match_id = None
                best_phonetic_canonical = None

                for ca in potential_phonetic_matches:
                     # Use fuzz.ratio as tiebreaker/confidence check
                     score = fuzz.ratio(normalized_address, ca['address'])
                     if score > best_phonetic_score and score >= PHONETIC_TIEBREAK_THRESHOLD:
                         best_phonetic_score = score
                         best_phonetic_match_id = ca['address_id']
                         best_phonetic_canonical = ca['address']

                if best_phonetic_match_id:
                    logging.debug(f"Best phonetic match found with score {best_phonetic_score:.2f}")
                    # Use the predefined confidence for phonetic matches, not the tiebreak score
                    return {
                        'address_id': best_phonetic_match_id,
                        'canonical_address': best_phonetic_canonical,
                        'match_type': 'phonetic',
                        'confidence_score': PHONETIC_MATCH_CONFIDENCE # Use the config value
                    }

        logging.debug("No match found after all methods.")
        return None # No match found

    except Exception as e:
        logging.error(f"Error during matching process: {e}")
        # Depending on requirements, you might want to return an internal server error
        return None
    finally:
        if cur:
            cur.close()


# Ensure phonetic keys are prepared when the app starts (or lazily)
# A simple way is to call it in the first request context.
phonetic_keys_prepared = False

@app.route('/match_address', methods=['POST'])
def match_address():
    """
    API endpoint to parse and match a single raw address.
    Expects a JSON body with a 'raw_address' key.
    """
    global phonetic_keys_prepared
    # Prepare phonetic keys lazily on the first request
    if not phonetic_keys_prepared:
        ensure_canonical_phonetic_keys()
        phonetic_keys_prepared = True

    data = request.get_json()
    if not data or 'raw_address' not in data:
        return jsonify({"error": "Invalid request. Please provide 'raw_address' in the JSON body."}), 400

    raw_address = data['raw_address']
    logging.info(f"Received address for matching: '{raw_address}'")

    # Step 1: Parse and Normalize
    parsed_components = parse_and_normalize_address(raw_address)

    if not parsed_components or not parsed_components.get('normalized_address'):
        logging.warning(f"Failed to parse or normalize address: '{raw_address}'")
        return jsonify({
            "raw_address": raw_address,
            "parsed_components": parsed_components if parsed_components else {},
            "normalized_address": None,
            "match": None,
            "unmatch_reason": "failed parse or normalization"
        }), 400 # 400 Bad Request if parsing fails

    normalized_address = parsed_components['normalized_address']
    logging.info(f"Normalized address: '{normalized_address}'")

    conn = None
    try:
        conn = get_db_connection()
        # Step 2: Attempt to Match
        match_result = find_best_match(conn, normalized_address)

        if match_result:
            logging.info(f"Match found: Type='{match_result['match_type']}', ID={match_result['address_id']}, Confidence={match_result['confidence_score']:.2f}")
            return jsonify({
                "raw_address": raw_address,
                "parsed_components": parsed_components,
                "normalized_address": normalized_address,
                "match": match_result,
                "unmatch_reason": None
            }), 200 # 200 OK

        else:
            logging.info(f"No match found for normalized address: '{normalized_address}'")
            return jsonify({
                "raw_address": raw_address,
                "parsed_components": parsed_components,
                "normalized_address": normalized_address,
                "match": None,
                "unmatch_reason": "no match found with sufficient confidence"
            }), 404 # 404 Not Found

    except Exception as e:
        logging.error(f"An error occurred while processing request for '{raw_address}': {e}")
        return jsonify({"error": "An internal error occurred.", "details": str(e)}), 500 # 500 Internal Server Error
    finally:
        if conn:
            close_db_connection(conn)

if __name__ == '__main__':
    # Run the Flask development server
    # In a production environment, use a production-ready WSGI server like Gunicorn or uWSGI
    logging.info("Starting Flask API server...")
    # The host='0.0.0.0' makes the server accessible externally. Use 127.0.0.1 for local only.
    app.run(debug=True, host='0.0.0.0', port=5000)