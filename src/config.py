# src/config.py
# Configuration settings for the address matching pipeline.
# Using environment variables is recommended for sensitive information like database credentials.

import os

class DatabaseConfig:
    """Database connection configuration."""
    DB_NAME = os.environ.get("DB_NAME", "address_matching_db")
    DB_USER = os.environ.get("DB_USER", "user")
    DB_PASSWORD = os.environ.get("DB_PASSWORD", "password")
    DB_HOST = os.environ.get("DB_HOST", "localhost")
    DB_PORT = os.environ.get("DB_PORT", "5432")

    @property
    def connection_string(self):
        """Returns a connection string for psycopg2."""
        return f"dbname={self.DB_NAME} user={self.DB_USER} password={self.DB_PASSWORD} host={self.DB_HOST} port={self.DB_PORT}"

class APIConfig:
    """External API configuration (mocked or real)."""
    # If using a real API, add keys and endpoints here.
    # For this example, we'll just have a flag for the mocked API.
    USE_MOCKED_API = os.environ.get("USE_MOCKED_API", "True").lower() == "true"
    # MOCKED_API_URL = "http://mocked-api.example.com/validate" # Example if needed

# Instantiate config objects
DB_CONFIG = DatabaseConfig()
API_CONFIG = APIConfig()

# Define input and output file paths
DATA_DIR = "data"
OUTPUT_DIR = os.path.join(DATA_DIR, "output")
CANONICAL_ADDRESSES_CSV = os.path.join(DATA_DIR, "11211 Addresses.csv")
TRANSACTIONS_CSV = os.path.join(DATA_DIR, "transactions_2_11211.csv")
SIMULATED_TRANSACTIONS_CSV = os.path.join(OUTPUT_DIR, "simulated_transactions.csv") # Output for simulation
FINAL_OUTPUT_CSV = os.path.join(OUTPUT_DIR, "output.csv")
UNMATCHED_REPORT_CSV = os.path.join(OUTPUT_DIR, "unmatched_report.csv")

# Matching thresholds
FUZZY_MATCH_THRESHOLD = 0.7 
PHONETIC_MATCH_CONFIDENCE = 0.6  
PHONETIC_TIEBREAK_THRESHOLD = 70

FTS_MATCH_THRESHOLD = 80
DEFAULT_MATCH_CONFIDENCE = 0.7