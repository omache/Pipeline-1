import usaddress
import re
import os
import csv
import logging
import psycopg2
from functools import lru_cache
from psycopg2.extras import execute_batch
from src.database import get_db_connection, close_db_connection

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Precompile regex patterns for better performance
STREET_NUMBER_PATTERN = re.compile(r'(\d+)')
STREET_NAME_PATTERN = re.compile(r'([a-zA-Z\s]+)')
STREET_TYPE_PATTERN = re.compile(r'(ST|AVE|BLVD|RD|LN|CT|DR|PL|WAY|TER|CIR|Street|Avenue|Boulevard|Road|Lane|Court|Drive|Place|Way|Terrace|Circle)')
DIRECTIONAL_PATTERN = re.compile(r'(N|S|E|W|NE|NW|SE|SW|NORTH|SOUTH|EAST|WEST|NORTHEAST|NORTHWEST|SOUTHEAST|SOUTHWEST)', re.IGNORECASE)
UNIT_PATTERN = re.compile(r'(Unit\s?[A-Za-z0-9]+)')

# Lookup dictionaries for fast mapping
STREET_TYPE_ABBREVIATIONS = {
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

DIRECTIONAL_ABBREVIATIONS = {
    'NORTH': 'N',
    'SOUTH': 'S',
    'EAST': 'E',
    'WEST': 'W',
    'NORTHEAST': 'NE',
    'NORTHWEST': 'NW',
    'SOUTHEAST': 'SE',
    'SOUTHWEST': 'SW'
}

# Use LRU cache to avoid re-processing identical addresses
@lru_cache(maxsize=10000)
def normalize_street_type(street_type):
    """Convert full street type to abbreviated form, only keeping ST or AVE."""
    if not street_type:
        return ''
    st_type = street_type.upper()
    abbreviated = STREET_TYPE_ABBREVIATIONS.get(st_type, street_type)
    
    # Only return ST or AVE, otherwise return empty string
    if abbreviated in ('ST', 'AVE'):
        return abbreviated
    return ''

@lru_cache(maxsize=1000)
def normalize_directional(directional):
    """Convert full directional name to abbreviated form."""
    if not directional:
        return ''
    dir_name = directional.upper()
    return DIRECTIONAL_ABBREVIATIONS.get(dir_name, directional)

def is_valid_unit(unit):
    """Check if unit is alphanumeric (contains both letters and numbers) or purely numeric."""
    if not unit:
        return False
    
    # Check if unit is purely numeric
    if unit.isdigit():
        return True
    
    # Check if unit is alphanumeric (contains both letters and numbers)
    return any(c.isalpha() for c in unit) and any(c.isdigit() for c in unit)

@lru_cache(maxsize=10000)
def parse_and_normalize_address(raw_address):
    """Normalize and parse address. Ensures 'unit' follows specific format rules."""
    if not raw_address or not isinstance(raw_address, str):
        return None

    # Initialize result dictionary with empty values
    result = {
        'street_name': '',
        'street_type': '',
        'pre_directional': '',
        'unit': '',
        'street_number': '',
        'normalized_address': ''
    }

    try:
        # First try usaddress parser
        parsed, _ = usaddress.tag(raw_address)
        
        # Extract components
        street_name = parsed.get('StreetName', '')
        street_type = parsed.get('StreetNamePostType', '')
        pre_directional = parsed.get('StreetNamePreDirectional', '')
        street_number = parsed.get('AddressNumber', '')
        
        # Process unit
        raw_unit_usaddress = parsed.get('OccupancyIdentifier', '')
        final_unit_value = ''
        
        if raw_unit_usaddress:
            # Keep only alphanumeric characters
            cleaned_unit = ''.join(char for char in raw_unit_usaddress if char.isalnum())
            if is_valid_unit(cleaned_unit):
                final_unit_value = cleaned_unit
        
        # Normalize components
        abbreviated_street_type = normalize_street_type(street_type)
        abbreviated_pre_directional = normalize_directional(pre_directional)
        
        # Build normalized address
        if street_name and street_type:
            if abbreviated_pre_directional:
                normalized = f"{street_number} {abbreviated_pre_directional} {street_name} {abbreviated_street_type}".strip()
            else:
                normalized = f"{street_number} {street_name} {abbreviated_street_type}".strip()
            
            if final_unit_value:
                normalized += f" APT {final_unit_value}"
            
            # Update result dictionary
            result.update({
                'street_name': street_name,
                'street_type': abbreviated_street_type,
                'pre_directional': abbreviated_pre_directional,
                'unit': final_unit_value,
                'street_number': street_number,
                'normalized_address': normalized.upper()
            })
            return result
            
        # If we got here, usaddress parser didn't provide enough info, raise to trigger regex fallback
        raise Exception("Incomplete usaddress parse")
            
    except Exception:
        # Fallback to regex parsing
        # Extract components using precompiled regex patterns
        street_number_match = STREET_NUMBER_PATTERN.search(raw_address)
        street_number = street_number_match.group(1).strip() if street_number_match else ''
        
        street_name_match = STREET_NAME_PATTERN.search(raw_address)
        street_name = street_name_match.group(1).strip() if street_name_match else ''
        
        street_type_match = STREET_TYPE_PATTERN.search(raw_address)
        street_type = street_type_match.group(1).strip() if street_type_match else ''
        
        pre_directional_match = DIRECTIONAL_PATTERN.search(raw_address)
        pre_directional = pre_directional_match.group(1).strip() if pre_directional_match else ''
        
        # Process unit
        unit_regex_match = UNIT_PATTERN.search(raw_address)
        raw_unit_regex = unit_regex_match.group(1).strip() if unit_regex_match else ''
        final_unit_value = ''

        if raw_unit_regex:
            unit_parts = raw_unit_regex.split()
            if len(unit_parts) > 1 and unit_parts[0].upper() == 'UNIT':
                potential_unit = ''.join(char for char in unit_parts[1] if char.isalnum())
                if is_valid_unit(potential_unit):
                    final_unit_value = potential_unit

        # Normalize components
        abbreviated_street_type = normalize_street_type(street_type)
        abbreviated_pre_directional = normalize_directional(pre_directional)
        
        # Build normalized address
        if abbreviated_pre_directional:
            normalized = f"{street_number} {abbreviated_pre_directional} {street_name} {abbreviated_street_type}".strip()
        else:
            normalized = f"{street_number} {street_name} {abbreviated_street_type}".strip()
            
        if final_unit_value:
            normalized += f" {final_unit_value}"
        
        # Update result dictionary
        result.update({
            'street_name': street_name,
            'street_type': abbreviated_street_type,
            'pre_directional': abbreviated_pre_directional,
            'unit': final_unit_value,
            'street_number': street_number,
            'normalized_address': normalized.upper()
        })
        return result

def ensure_address_columns_exist(conn):
    """Check if necessary columns exist in the transactions table and add them if not."""
    logging.info("Checking if necessary columns exist in transactions table")
    
    with conn.cursor() as cursor:
        # Get existing columns in a single operation
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'transactions';
        """)
        existing_columns = {col[0].lower() for col in cursor.fetchall()}
        
        # Define required columns
        required_columns = {
            'parsed_street_number': 'VARCHAR(30)',
            'parsed_street_name': 'VARCHAR(100)',
            'parsed_street_suffix': 'VARCHAR(30)',
            'parsed_pre_directional': 'VARCHAR(10)',
            'parsed_unit': 'VARCHAR(30)',
            'normalized_address': 'VARCHAR(200)',
            'unmatch_reason': 'VARCHAR(100)'
        }
        
        # Build a single ALTER TABLE statement for all missing columns
        missing_columns = []
        for column, data_type in required_columns.items():
            if column.lower() not in existing_columns:
                missing_columns.append(f"ADD COLUMN {column} {data_type}")
        
        if missing_columns:
            alter_statement = f"ALTER TABLE transactions {', '.join(missing_columns)};"
            cursor.execute(alter_statement)
            logging.info(f"Added {len(missing_columns)} missing columns")
        else:
            logging.info("All necessary columns already exist")
        
        conn.commit()

def parse_all_transactions():
    """Main function to parse addresses from the database and update the transactions table."""
    logging.info("Starting parsing of transaction addresses.")
    
    # Set up CSV output file
    output_file = 'data/output/parsed_data.csv'
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    csv_header = ['id', 'raw_address', 'parsed_street_number', 'parsed_pre_directional', 
                 'parsed_street_name', 'parsed_street_suffix', 'parsed_unit', 'normalized_address']
    
    try:
        # Get database connection and ensure columns exist
        conn = get_db_connection()
        ensure_address_columns_exist(conn)
        
        # For better performance, we'll use a single connection with multiple cursors
        with conn.cursor() as select_cur, conn.cursor() as update_cur, \
             open(output_file, 'w', newline='', encoding='utf-8') as csv_file:
            
            # Set up CSV writer
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(csv_header)
            
            # Optimize query - only fetch addresses that need parsing
            select_cur.execute("""
                SELECT id, address_line_1, address_line_2
                FROM transactions
                WHERE (parsed_street_number IS NULL OR parsed_street_name IS NULL OR parsed_unit IS NULL)
                OR unmatch_reason = 'failed parse';
            """)
            
            # Process in batches for better memory management
            batch_size = 10000  # Larger batch size for better throughput
            processed_count = 0
            updated_count = 0
            
            # Prepare arrays for batch processing
            batch = select_cur.fetchmany(batch_size)
            
            while batch:
                logging.info(f"Processing batch of {len(batch)} transactions (total processed: {processed_count}).")
                update_data = []
                csv_rows = []
                
                for id, address_line_1, address_line_2 in batch:
                    # Combine address lines efficiently
                    address_parts = []
                    if address_line_1:
                        address_parts.append(address_line_1)
                    if address_line_2:
                        address_parts.append(address_line_2)
                    
                    full_address = ' '.join(address_parts)
                    
                    if not full_address:
                        # Handle empty addresses
                        update_data.append(('', '', '', '', '', '', "empty address", id))
                        csv_rows.append([id, '', '', '', '', '', '', ''])
                    else:
                        # Parse address
                        parsed_components = parse_and_normalize_address(full_address)
                        
                        if parsed_components:
                            # Add to batch update data
                            update_data.append((
                                parsed_components['street_number'],
                                parsed_components['street_name'],
                                parsed_components['street_type'],
                                parsed_components['pre_directional'],
                                parsed_components['unit'],
                                parsed_components['normalized_address'],
                                None,  # unmatch_reason is NULL on success
                                id
                            ))
                            
                            # Add to CSV rows
                            csv_rows.append([
                                id,
                                full_address,
                                parsed_components['street_number'],
                                parsed_components['pre_directional'],
                                parsed_components['street_name'],
                                parsed_components['street_type'],
                                parsed_components['unit'],
                                parsed_components['normalized_address']
                            ])
                            updated_count += 1
                        else:
                            # Handle failed parse
                            update_data.append(('', '', '', '', '', '', "failed parse (invalid input)", id))
                            csv_rows.append([id, full_address, '', '', '', '', '', ''])
                    
                    processed_count += 1
                
                # Batch update database
                if update_data:
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
                        update_data,
                        page_size=1000  # Optimal page size for database
                    )
                    conn.commit()
                    logging.info(f"Committed batch. Updated: {len(update_data)} rows")
                
                # Write to CSV file in batches
                csv_writer.writerows(csv_rows)
                
                # Get next batch
                batch = select_cur.fetchmany(batch_size)
        
        logging.info(f"Parsing complete. {processed_count} records processed, {updated_count} records updated.")
        
    except Exception as e:
        logging.error(f"Error processing addresses: {e}")
        raise
    finally:
        if conn:
            close_db_connection(conn)

if __name__ == "__main__":
    try:
        logging.info("Starting parsing script.")
        parse_all_transactions()
        logging.info("Parsing script finished successfully.")
    except Exception as e:
        logging.error(f"Parsing script failed: {e}")