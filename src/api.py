# src/api.py
# A simple Flask REST endpoint for matching a single address.

from flask import Flask, request, jsonify
from src.database import get_db_connection, close_db_connection
from src.parse import parse_and_normalize_address
from src.fallback import mock_external_api_validate # Assuming this exists as per comments
import logging
from rapidfuzz import fuzz
import jellyfish
import psycopg2 # Added for specific exception handling

# Configure logging for the API
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

# Placeholder for a constant that should ideally be in a config file or defined globally
FUZZY_MATCH_THRESHOLD = 85.0 # Example threshold (0-100)

# --- Helper function to perform matching for a single address ---
# This function encapsulates the matching logic for one address.
# It's a simplified version of the logic in match.py and fallback.py
# and queries the database for canonical addresses as needed.
def match_single_address(raw_address):
    """
    Matches a single raw address against the canonical addresses in the database.
    Follows the matching waterfall (exact, fuzzy, phonetic, mocked API).

    Args:
        raw_address (str): The raw address string to match.

    Returns:
        dict: A dictionary containing the match result:
              {'matched_address_id': id or None,
               'match_type': 'exact', 'fuzzy', 'phonetic', 'api', 'unmatched',
               'confidence_score': float or None,
               'unmatch_reason': str or None}
    """
    logging.info(f"Attempting to match single address: {raw_address}")

    # 1. Parse and Normalize
    # Output: (parsed_street_number, parsed_street_name, parsed_street_suffix, 
    #          parsed_unit, parsed_city, parsed_state, parsed_zip)
    parsed_components = parse_and_normalize_address(raw_address)

    if not parsed_components:
        logging.warning(f"Parsing failed for single address: {raw_address}")
        return {
            'matched_address_id': None,
            'match_type': 'unmatched',
            'confidence_score': None,
            'unmatch_reason': 'failed parse'
        }

    (
        parsed_street_number, # Corresponds to 'house' in canonical_addresses
        parsed_street_name,   # Corresponds to 'street' in canonical_addresses
        _parsed_street_suffix,# Corresponds to 'strtype', not used in current SQL match logic
        _parsed_unit,         # Corresponds to 'aptnbr', not used in current SQL match logic
        _parsed_city,         # Corresponds to 'city', not used in current SQL match logic
        _parsed_state,        # Corresponds to 'state', not used in current SQL match logic
        parsed_zip            # Corresponds to 'zip' in canonical_addresses
    ) = parsed_components

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        match_result = {
            'matched_address_id': None,
            'match_type': None,
            'confidence_score': None,
            'unmatch_reason': None
        }

        # 2. Exact Matching
        # Uses parsed_street_number (for house), parsed_street_name (for street), and parsed_zip (for zip)
        if parsed_street_number and parsed_street_name and parsed_zip:
            logging.debug("Attempting exact match.")
            exact_match_query = """
            SELECT address_id
            FROM canonical_addresses
            WHERE
                house = %s AND    -- Schema: house
                street = %s AND   -- Schema: street
                zip = %s          -- Schema: zip
            LIMIT 1;
            """
            cur.execute(exact_match_query, (parsed_street_number, parsed_street_name, parsed_zip))
            exact_match = cur.fetchone()

            if exact_match:
                logging.debug(f"Exact match found: {exact_match[0]}")
                match_result['matched_address_id'] = exact_match[0]
                match_result['match_type'] = 'exact'
                match_result['confidence_score'] = 1.0
                cur.close() # Close cursor after use
                return match_result # Return immediately on exact match

        # 3. Fuzzy Matching (with simple zip block)
        # Uses parsed_street_name (for street) and parsed_zip (for zip)
        if parsed_street_name and parsed_zip:
            logging.debug("Attempting fuzzy match.")
            # Fetch canonical addresses in the same zip code
            cur.execute("""
                SELECT address_id, street  -- Schema: street
                FROM canonical_addresses
                WHERE zip = %s AND street IS NOT NULL; -- Schema: zip, street
            """, (parsed_zip,))
            canonical_in_block = cur.fetchall()

            best_match_id = None
            highest_score = 0.0 

            for canon_id, canon_name in canonical_in_block:
                if canon_name: # Ensure canon_name is not None before fuzz.ratio
                    score = fuzz.ratio(parsed_street_name, canon_name) / 100.0 # Normalize to 0-1
                    if score > highest_score:
                        highest_score = score
                        best_match_id = canon_id
            
            # rapidfuzz.fuzz.ratio returns 0-100, so threshold is 0-100
            if best_match_id is not None and (highest_score * 100.0) >= FUZZY_MATCH_THRESHOLD:
                logging.debug(f"Fuzzy match found: {best_match_id} with score {highest_score*100.0}")
                match_result['matched_address_id'] = best_match_id
                match_result['match_type'] = 'fuzzy'
                match_result['confidence_score'] = highest_score 
                cur.close() # Close cursor after use
                return match_result # Return on fuzzy match

        # 4. Phonetic Matching (with simple zip block)
        # Uses parsed_street_name (for street) and parsed_zip (for zip)
        if parsed_street_name and parsed_zip:
            logging.debug("Attempting phonetic match.")
            trans_soundex = jellyfish.soundex(parsed_street_name)

            # Fetch canonical addresses in the same zip code (could reuse previous fetch if structured differently)
            # For clarity, re-fetching here or ensure cursor is still valid if not re-fetched.
            # If previous fuzzy search did not return, cursor is still open from that section.
            # Let's assume we need to re-execute or ensure cursor state.
            # For safety, creating a new query execution path or reusing fetched data carefully.
            # The current code structure implies the cursor is still open if no fuzzy match was returned.
            
            # If fuzzy matching section did not execute (e.g. missing parsed_street_name),
            # cursor for this specific query might not have run.
            # If it did run and didn't find match, canonical_in_block could be reused.
            # For simplicity and to ensure correct data, let's re-query if not already fetched or if fuzzy was skipped.
            # However, the current logic flows, so canonical_in_block from fuzzy section would be available if that path was taken.
            # If fuzzy path (parsed_street_name and parsed_zip) was not taken, this path also won't be.
            # So, we can assume canonical_in_block is populated if this stage is reached.
            
            # If `canonical_in_block` was not defined because fuzzy stage was skipped, this will error.
            # Better to ensure data is fetched for this stage if needed, or rely on `canonical_in_block` if fuzzy stage ran.
            # Let's assume `canonical_in_block` from the fuzzy section is available if this part is reached.
            if not 'canonical_in_block' in locals() or canonical_in_block is None: # Ensure it was fetched
                cur.execute("""
                    SELECT address_id, street  -- Schema: street
                    FROM canonical_addresses
                    WHERE zip = %s AND street IS NOT NULL; -- Schema: zip, street
                """, (parsed_zip,))
                canonical_in_block = cur.fetchall()


            best_phonetic_match_id = None
            for canon_id, canon_name in canonical_in_block:
                if canon_name: # Ensure canon_name is not None
                    canon_soundex = jellyfish.soundex(canon_name)
                    if trans_soundex == canon_soundex:
                        best_phonetic_match_id = canon_id
                        break 

            if best_phonetic_match_id is not None:
                logging.debug(f"Phonetic match found: {best_phonetic_match_id}")
                match_result['matched_address_id'] = best_phonetic_match_id
                match_result['match_type'] = 'phonetic'
                # Assign a confidence score (e.g., lower than exact/good fuzzy, higher than API if it's just a fallback)
                # For simplicity, we use a fixed score for phonetic match. Consider Jaro-Winkler for refinement.
                match_result['confidence_score'] = 0.85 # Example score for phonetic
                cur.close() # Close cursor after use
                return match_result


        # 5. Fallback: Mocked External API
        logging.debug("Attempting mocked API fallback.")
        api_result = mock_external_api_validate(raw_address) 

        if api_result:
            if api_result.get('status') == 'success' and api_result.get('matched_address_id') is not None:
                # Mock API returned a match. Verify if this address_id exists in our canonical table.
                # A new cursor might be needed if the previous one was closed, or ensure it's still open.
                # The main cursor 'cur' should still be open here.
                cur.execute("SELECT address_id FROM canonical_addresses WHERE address_id = %s LIMIT 1;", 
                              (api_result['matched_address_id'],))
                canonical_match = cur.fetchone()
                
                if canonical_match:
                    logging.debug(f"Mock API match found and confirmed in canonical: {canonical_match[0]}")
                    match_result['matched_address_id'] = canonical_match[0]
                    match_result['match_type'] = 'api'
                    match_result['confidence_score'] = api_result.get('confidence', 0.75) # Example API confidence
                    cur.close() # Close cursor
                    return match_result
                else:
                    logging.debug(f"Mock API match found ({api_result['matched_address_id']}) but not in our canonical_addresses table.")
                    match_result['unmatch_reason'] = 'api match not in canonical_data'
            elif api_result.get('status') == 'success': # Success but no matched_address_id
                 match_result['unmatch_reason'] = 'api validation success no match'
            elif api_result.get('status') == 'rate_limited':
                logging.warning("Mock API rate-limited during single match.")
                match_result['unmatch_reason'] = 'api rate_limited'
            elif api_result.get('status') == 'error':
                logging.error(f"Mock API error during single match: {api_result.get('message', 'unknown API error')}")
                match_result['unmatch_reason'] = f"api error: {api_result.get('message', 'unknown API error')}"
            else: # Other statuses or unexpected structure
                logging.error(f"Mock API unexpected response: {api_result}")
                match_result['unmatch_reason'] = 'api failed or unexpected_response'
        else: # api_result is None
            logging.error("Mock API call returned None.")
            match_result['unmatch_reason'] = 'api call failed (no response)'


        # 6. No Match Found
        if match_result['matched_address_id'] is None:
            logging.debug("No match found after all strategies.")
            if match_result['unmatch_reason'] is None: # If no specific reason set yet
                match_result['unmatch_reason'] = 'no match found by any strategy'
            match_result['match_type'] = 'unmatched' 

        cur.close() # Ensure cursor is closed if not already
        return match_result

    except psycopg2.Error as e:
        logging.error(f"Database error during single address matching: {e}")
        return {
            'matched_address_id': None, 'match_type': 'unmatched',
            'confidence_score': None, 'unmatch_reason': f'database error: {str(e)}'
        }
    except Exception as e:
        logging.error(f"An unexpected error occurred during single address matching: {e}", exc_info=True)
        return {
            'matched_address_id': None, 'match_type': 'unmatched',
            'confidence_score': None, 'unmatch_reason': f'internal error: {str(e)}'
        }
    finally:
        if conn:
            # Ensure cursor is closed if an exception occurred before explicit close
            # (though individual sections try to close it on successful return)
            if 'cur' in locals() and cur and not cur.closed:
                cur.close()
            close_db_connection(conn)


# --- Flask Endpoint ---
@app.route('/match_address', methods=['POST'])
def match_address_endpoint():
    """
    REST endpoint to match a single address.
    Expects a JSON payload with a 'raw_address' key.
    Returns a JSON response with the match result.
    """
    if not request.is_json:
        return jsonify({"error": "Request must be JSON", "status_code": 415}), 415

    data = request.get_json()
    raw_address = data.get('raw_address')

    if not raw_address or not isinstance(raw_address, str) or not raw_address.strip():
        return jsonify({"error": "Missing or invalid 'raw_address' (must be a non-empty string) in request body", "status_code": 400}), 400

    # Perform the single address matching logic
    match_response = match_single_address(raw_address)

    # Add the original raw address to the response for context
    match_response['original_raw_address'] = raw_address
    
    # Determine HTTP status code based on match outcome
    if match_response.get('matched_address_id') is not None:
        status_code = 200 # OK - Match found
    elif match_response.get('unmatch_reason', '').startswith('database error') or \
         match_response.get('unmatch_reason', '').startswith('internal error'):
        status_code = 500 # Internal Server Error
    elif match_response.get('unmatch_reason') == 'failed parse':
        status_code = 400 # Bad Request - Parsing failed
    else:
        status_code = 200 # OK - Processed, but no match found (could also be 404 if preferred for "not found")

    return jsonify(match_response), status_code

if __name__ == '__main__':
    logging.info("Starting Flask API server for address matching.")
    # For production, use a WSGI server like Gunicorn or uWSGI.
    # Example POST request using curl:
    # curl -X POST -H "Content-Type: application/json" -d '{"raw_address": "123 Main St Brooklyn NY 11211"}' http://127.0.0.1:5000/match_address
    app.run(debug=False, host='0.0.0.0', port=5000) # Set debug=False for production