import usaddress
import re
import os
import csv
import logging
import psycopg2
from src.database import get_db_connection, close_db_connection
from psycopg2.extras import execute_batch # Import execute_batch


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

street_number_pattern = r'(\d+)'
street_name_pattern = r'([a-zA-Z\s]+)' 
street_type_pattern = r'(ST|AVE|BLVD|RD|LN|CT|DR|PL|WAY|TER|CIR|Street|Avenue|Boulevard|Road|Lane|Court|Drive|Place|Way|Terrace|Circle)'
# New pattern for directionals
directional_pattern = r'(N|S|E|W|NE|NW|SE|SW|NORTH|SOUTH|EAST|WEST|NORTHEAST|NORTHWEST|SOUTHEAST|SOUTHWEST)'

# Dictionary to map full street types to abbreviations
street_type_abbreviations = {
    'STREET': 'ST',
    'AVENUE': 'AVE',
    'BOULEVARD': 'BLVD',
    'ROAD': 'RD',
    'LANE': 'LN',
    'COURT': 'CT',
    'DRIVE': 'DR',
    'PLACE': 'PL',
    'WAY': 'WAY',
    'TERRACE': 'TER',
    'CIRCLE': 'CIR'
}

# Dictionary to map full directional names to abbreviations
directional_abbreviations = {
    'NORTH': 'N',
    'SOUTH': 'S',
    'EAST': 'E',
    'WEST': 'W',
    'NORTHEAST': 'NE',
    'NORTHWEST': 'NW',
    'SOUTHEAST': 'SE',
    'SOUTHWEST': 'SW'
}

def normalize_street_type(street_type):
    """Convert full street type to abbreviated form, only keeping ST or AVE."""
    if not street_type:
        return ''
    st_type = street_type.upper()
    abbreviated = street_type_abbreviations.get(st_type, street_type)
    
    # Only return ST or AVE, otherwise return empty string
    if abbreviated == 'ST' or abbreviated == 'AVE':
        return abbreviated
    return ''

def normalize_directional(directional):
    """Convert full directional name to abbreviated form."""
    if not directional:
        return ''
    dir_name = directional.upper()
    return directional_abbreviations.get(dir_name, directional)

def is_valid_unit(unit):
    """Check if unit is alphanumeric (contains both letters and numbers) or purely numeric."""
    if not unit:
        return False
        
    # Check if unit is purely numeric
    if unit.isdigit():
        return True
        
    # Check if unit is alphanumeric (contains both letters and numbers)
    has_letter = any(c.isalpha() for c in unit)
    has_digit = any(c.isdigit() for c in unit)
    
    return has_letter and has_digit

def parse_and_normalize_address(raw_address):
    """Normalize and parse address. Ensures 'unit' follows specific format rules."""
    if not raw_address or not isinstance(raw_address, str):
        logging.warning(f"Invalid address input: {raw_address}")
        return None

    final_unit_value = '' # Initialize to blank

    try:
        parsed, _ = usaddress.tag(raw_address)
        street_name = parsed.get('StreetName', '')
        street_type = parsed.get('StreetNamePostType', '')
        pre_directional = parsed.get('StreetNamePreDirectional', '')
        street_number = parsed.get('AddressNumber', '')
        
        # Process OccupancyIdentifier (unit) from usaddress
        raw_unit_usaddress = parsed.get('OccupancyIdentifier') # This can be None or a string

        if raw_unit_usaddress and isinstance(raw_unit_usaddress, str):
            # Remove any non-alphanumeric characters (keeping only letters and numbers)
            cleaned_unit = ''.join(char for char in raw_unit_usaddress if char.isalnum())
            if is_valid_unit(cleaned_unit):
                final_unit_value = cleaned_unit
        
        abbreviated_street_type = normalize_street_type(street_type)
        abbreviated_pre_directional = normalize_directional(pre_directional)
        
        if abbreviated_pre_directional:
            normalized = f"{street_number} {abbreviated_pre_directional} {street_name} {abbreviated_street_type}".strip()
        else:
            normalized = f"{street_number} {street_name} {abbreviated_street_type}".strip()
            
        if final_unit_value: # Append if unit is not blank
            normalized += f" APT {final_unit_value}" # usaddress path usually implies "APT" or similar prefix contextually

        if street_name and street_type: # Original condition for successful usaddress parse
            return {
                'street_name': street_name,
                'street_type': abbreviated_street_type,
                'pre_directional': abbreviated_pre_directional,
                'unit': final_unit_value, # Use the cleaned unit value
                'street_number': street_number,
                'normalized_address': normalized.upper()
            }
        else:
            raise Exception("Parsing failed with usaddress (e.g., missing street_name or street_type).")
            
    except (usaddress.RepeatedLabelError, Exception) as e:
        # Fallback to regex parsing
        street_number_match = re.search(street_number_pattern, raw_address)
        street_number = street_number_match.group(1).strip() if street_number_match else ''
        
        # Note: street_name_pattern is greedy. May need refinement depending on address structures.
        street_name_match = re.search(street_name_pattern, raw_address)
        street_name = street_name_match.group(1).strip() if street_name_match else ''
        
        street_type_match = re.search(street_type_pattern, raw_address)
        street_type = street_type_match.group(1).strip() if street_type_match else '' # Ensure strip
        
        pre_directional_match = re.search(directional_pattern, raw_address, re.IGNORECASE)
        pre_directional = pre_directional_match.group(1).strip() if pre_directional_match else '' # Ensure strip
        
        # Process unit from regex
        # Original regex: r'(Unit\s?[A-Za-z0-9]+)' captures "Unit XYZ".
        unit_regex_match = re.search(r'(Unit\s?[A-Za-z0-9]+)', raw_address)
        raw_unit_regex = unit_regex_match.group(1).strip() if unit_regex_match else ''

        if raw_unit_regex:
            # Extract the unit number after "Unit" if it exists
            unit_parts = raw_unit_regex.split()
            if len(unit_parts) > 1 and unit_parts[0].upper() == 'UNIT':
                potential_unit = ''.join(char for char in unit_parts[1] if char.isalnum())
                if is_valid_unit(potential_unit):
                    final_unit_value = potential_unit

        abbreviated_street_type = normalize_street_type(street_type)
        abbreviated_pre_directional = normalize_directional(pre_directional)
        
        if abbreviated_pre_directional:
            normalized = f"{street_number} {abbreviated_pre_directional} {street_name} {abbreviated_street_type}".strip()
        else:
            normalized = f"{street_number} {street_name} {abbreviated_street_type}".strip()
            
        if final_unit_value: # Append if unit is not blank
            normalized += f" {final_unit_value}" # Regex fallback appends unit directly

        return {
            'street_name': street_name,
            'street_type': abbreviated_street_type,
            'pre_directional': abbreviated_pre_directional,
            'unit': final_unit_value, # Use the cleaned unit value
            'street_number': street_number,
            'normalized_address': normalized.upper()
        }

def ensure_address_columns_exist(conn):
    """Check if necessary columns exist in the transactions table and add them if not."""
    logging.info("Checking if necessary columns exist in transactions table")
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'transactions';
        """)
        existing_columns = [col[0] for col in cursor.fetchall()]
        
        required_columns = {
            'parsed_street_number': 'VARCHAR(30)',
            'parsed_street_name': 'VARCHAR(100)',
            'parsed_street_suffix': 'VARCHAR(30)',
            'parsed_pre_directional': 'VARCHAR(10)',
            'parsed_unit': 'VARCHAR(30)',
            'normalized_address': 'VARCHAR(200)',
            'unmatch_reason': 'VARCHAR(100)'
        }
        
        for column, data_type in required_columns.items():
            if column.lower() not in [col.lower() for col in existing_columns]:
                logging.info(f"Adding column {column} to transactions table")
                cursor.execute(f"""
                    ALTER TABLE transactions 
                    ADD COLUMN {column} {data_type};
                """)
        
        conn.commit()
        logging.info("All necessary columns are now in place")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error ensuring columns exist: {e}")
        raise
    finally:
        cursor.close()

def parse_all_transactions():
    """Main function to parse addresses from the database and update the transactions table."""
    logging.info("Starting parsing of transaction addresses.")
    conn = None
    select_cur = None
    update_cur = None
    output_file = 'data/output/parsed_data.csv'
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # ... (CSV header and initial setup) ...
    csv_rows = []
    csv_header = ['id', 'raw_address', 'parsed_street_number', 'parsed_pre_directional', 'parsed_street_name', 'parsed_street_suffix', 'parsed_unit', 'normalized_address']
    csv_rows.append(csv_header)

    try:
        conn = get_db_connection()
        ensure_address_columns_exist(conn)

        select_cur = conn.cursor()
        update_cur = conn.cursor()

        select_cur.execute("""
            SELECT id, address_line_1, address_line_2
            FROM transactions
            WHERE (parsed_street_number IS NULL OR parsed_street_name IS NULL OR parsed_unit IS NULL)
            OR unmatch_reason = 'failed parse';
        """)

        batch_size = 5000 # Experiment with this value
        processed_count = 0
        updated_count = 0
        update_data = [] # List to hold data for batch updates

        while True:
            batch = select_cur.fetchmany(batch_size)
            if not batch:
                break

            logging.info(f"Processing batch of {len(batch)} transactions (total processed: {processed_count}).")

            for id, address_line_1, address_line_2 in batch:
                full_address = f"{address_line_1 or ''} {address_line_2 or ''}".strip()

                parsed_components = None # Initialize to None

                if not full_address:
                    logging.warning(f"Skipping parsing for transaction {id} due to empty address.")
                    unmatch_reason = "empty address"
                    update_data.append((
                        '', '', '', '', '', '', unmatch_reason, id
                    ))
                    csv_rows.append([id, full_address, '', '', '', '', '', ''])
                else:
                    parsed_components = parse_and_normalize_address(full_address)

                    if parsed_components:
                         update_data.append((
                            parsed_components['street_number'] or '',
                            parsed_components['street_name'] or '',
                            parsed_components['street_type'] or '',
                            parsed_components['pre_directional'] or '',
                            parsed_components['unit'] or '',
                            parsed_components['normalized_address'] or '',
                            None, # unmatch_reason is NULL on success
                            id
                        ))
                         csv_rows.append([
                            id,
                            full_address,
                            parsed_components['street_number'] or '',
                            parsed_components['pre_directional'] or '',
                            parsed_components['street_name'] or '',
                            parsed_components['street_type'] or '',
                            parsed_components['unit'] or '',
                            parsed_components['normalized_address'] or ''
                        ])
                         updated_count += 1
                    else:
                        unmatch_reason = "failed parse (invalid input)"
                        update_data.append((
                            '', '', '', '', '', '', unmatch_reason, id
                        ))
                        csv_rows.append([id, full_address, '', '', '', '', '', ''])

                processed_count += 1

            # Execute batch update after processing the batch
            if update_data:
                logging.info(f"Executing batch update for {len(update_data)} rows.")
                execute_batch(
                    update_cur,
                    """
                    UPDATE transactions
                    SET parsed_street_number = %s,
                        parsed_street_name = %s,
                        parsed_street_suffix = %s,
                        parsed_pre_directional = %s,
                        parsed_unit = %s,
                        normalized_address = %s,
                        unmatch_reason = %s
                    WHERE id = %s;
                    """,
                    update_data
                )
                conn.commit()
                logging.info(f"Committed batch. Total processed: {processed_count}, Updated: {updated_count}")
                update_data = [] # Clear the batch data list

        # Commit any remaining updates if the last batch was smaller than batch_size
        if update_data:
             logging.info(f"Executing final batch update for {len(update_data)} rows.")
             execute_batch(
                update_cur,
                """
                UPDATE transactions
                SET parsed_street_number = %s,
                    parsed_street_name = %s,
                    parsed_street_suffix = %s,
                    parsed_pre_directional = %s,
                    parsed_unit = %s,
                    normalized_address = %s,
                    unmatch_reason = %s
                WHERE id = %s;
                """,
                update_data
            )
             conn.commit()
             logging.info(f"Committed final batch. Total processed: {processed_count}, Updated: {updated_count}")


        logging.info("Parsing complete. Writing results to CSV.")

        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(csv_rows)

        logging.info(f"Parsing complete. {processed_count} records processed, {updated_count} records updated in database.")

    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        logging.error(f"Database error: {e}")
        raise
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Unexpected error: {e}")
        raise
    finally:
        if select_cur:
            select_cur.close()
        if update_cur:
            update_cur.close()
        if conn:
            close_db_connection(conn)

if __name__ == "__main__":
    try:
        logging.info("Starting parsing script.")
        parse_all_transactions()
        logging.info("Parsing script finished.")
    except Exception as e:
        logging.error(f"Parsing script failed: {e}")