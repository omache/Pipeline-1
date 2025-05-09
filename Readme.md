# Address Matching Pipeline

This project implements a scalable address matching pipeline designed to ingest transaction and canonical address data, normalize and parse addresses, and match transactions to canonical records using a multi-stage waterfall approach (Exact, Fuzzy, Phonetic). It is containerized using Docker and `docker-compose` for easy setup and execution.

## Deliverables

This repository contains the following components:

* **Ingestion Scripts:** `src/ingest.py` handles reading CSVs and loading data into PostgreSQL.
* **Matching Logic:** Modular scripts including `src/parse.py` (parsing/normalization), `src/match.py` (exact and fuzzy matching), and `src/fallback_match.py` (phonetic fallback matching).
* **Schema DDL:** `sql/schema.sql` defines the database tables and indexes.
* **Performance Harness:** `src/run_pipeline.py` orchestrates the full workflow and includes basic timing and memory profiling capabilities. `src/simulate_data.py` is used to generate large test datasets.
* **Configuration:** `requirements.txt` lists Python dependencies, `Dockerfile` defines the application container image, and `docker-compose.yml` orchestrates the services.

## Setup Instructions

The project is designed to run within Docker containers.

1.  **Prerequisites:**
    * Install Docker and Docker Compose: [https://docs.docker.com/get-docker/](https://docs.docker.com/get-docker/)
2.  **Clone the Repository:**
    ```bash
    git clone <your_repository_url>
    cd <your_repository_directory>
    ```
3.  **Prepare Input Data:**
    * Place your `11211 Addresses.csv` and `transactions_2_11211.csv` files inside the `./data/input` directory.
4.  **Create Environment File:**
    * Create a `.env` file in the project root directory (same level as `docker-compose.yml`).
    * Define your database credentials and names here. These will be used by `docker-compose` to configure the database and application containers.
    ```env
    DB_NAME=address_matching_db
    DB_USER=user
    DB_PASSWORD=password
    ```
    * You can customize these values, but ensure they match the defaults or are correctly picked up by `src/config.py`.

## How to Run End-to-End

1.  **Build the Docker Image:**
    ```bash
    docker-compose build
    ```
    This will build the application image based on the `Dockerfile`.
2.  **Start the Services:**
    ```bash
    docker-compose up -d db app
    ```
    This starts the database and application containers in detached mode (`-d`). `depends_on: db` in the `docker-compose.yml` ensures the database starts before the application.
3.  **Initialize the Database Schema:**
    * Run the SQL schema script inside the database container. This creates the tables and indexes.
    ```bash
    docker-compose exec db psql -d ${DB_NAME} -U ${DB_USER} -f /app/sql/schema.sql
    ```
    Replace `${DB_NAME}` and `${DB_USER}` with the values from your `.env` file or command line if you're not using `.env`. You may be prompted for the password.
4.  **(Optional) Simulate Large Data:**
    * If you want to test with a larger dataset (e.g., 200 million rows), run the simulation script. This will create `data/output/simulated_transactions.csv`.
    ```bash
    docker-compose exec app python /app/src/simulate_data.py
    # By default, this simulates 50000 rows. Edit src/simulate_data.py or
    # modify the run_pipeline.py call to simulate_large_transactions_csv
    # to change the target_rows.
    ```
5.  **Run the Full Pipeline:**
    * Execute the main pipeline script inside the application container.
    * To use the *original* small dataset:
        ```bash
        docker-compose exec app python /app/run_pipeline.py
        ```
    * To use the *simulated large* dataset:
        ```bash
        docker-compose exec app python /app/run_pipeline.py --simulate
        ```
    * To run the pipeline with memory profiling (using the original dataset by default, add `--simulate` for simulated data):
        ```bash
        docker-compose exec app python /app/run_pipeline.py --profile-memory [--simulate]
        ```
6.  **View Results:**
    * The output files will be generated in your local `./data/output` directory:
        * `output.csv`: Final matched transaction details.
        * `unmatched_report.csv`: Details of records that could not be matched.
        * `parsed_data.csv`: Intermediate output from the parsing step.
        * `simulated_transactions.csv`: The generated large dataset (if simulation was run).

7.  **Stop Services:**
    ```bash
    docker-compose down
    ```
    This stops and removes the containers and networks (but preserves the `db_data` volume by default).

## Performance Results

*(Please replace the following placeholders with your actual results after running the pipeline on your local machine or cloud environment)*

* **Total Runtime:** [Insert Measured Total Runtime Here] seconds
* **Peak Memory:** [Insert Measured Peak Memory Here from `--profile-memory` run output] MB
* **Approximate Cost:** [Estimate cost if using cloud resources or external APIs, otherwise state N/A]

To obtain peak memory usage, run the pipeline with the `--profile-memory` flag as described in the "How to Run" section and examine the output logs.

## Design Write-up

### Database Choice

* **Choice:** PostgreSQL with PostGIS and pg_trgm extensions.
* **Trade-offs:**
    * **Pros:** Robust, mature RDBMS. PostGIS provides powerful geospatial capabilities (though not heavily used in the current matching logic, the schema includes it). `pg_trgm` offers highly optimized in-database trigram similarity functions and GIN indexes crucial for efficient fuzzy matching on large text fields, avoiding the need to transfer large amounts of data to the application layer for string comparisons. Well-supported by Python libraries (`psycopg2`).
    * **Cons:** Requires setup and management of a database server (simplified by Docker). Might be less performant than specialized search databases (like Elasticsearch/OpenSearch) for *very* complex full-text search scenarios, but `pg_trgm` is highly effective for address similarity.

### Libraries/APIs Used

* **Python Standard Library:** `os`, `sys`, `logging`, `time`, `subprocess`, `functools.lru_cache`.
* **Third-party Libraries:**
    * `pandas`: For efficient CSV reading, writing, and data manipulation.
    * `psycopg2`: PostgreSQL adapter for Python. Used for database connections, executing queries, and efficient batch operations (`psycopg2.extras.execute_values`).
    * `usaddress`: Python library for parsing unstructured US address strings into components.
    * `jellyfish`: Python library for phonetic algorithms (Metaphone, Soundex). Used for generating phonetic keys for fallback matching.
    * `rapidfuzz`: Python library for fast fuzzy string matching (Levenshtein distance, Jaro-Winkler, etc.). Used specifically for the `fuzz.ratio` tie-breaking in the phonetic matching stage.
    * `memory-profiler`: Used as a tool via subprocess to measure memory usage of the pipeline script.
* **Database Features:**
    * PostGIS: Geospatial extension (schema included, but not actively used in matching logic).
    * pg_trgm: PostgreSQL extension for trigram-based similarity. Used for efficient fuzzy matching (`similarity`, `word_similarity`) and powered by GIN indexes.
    * `execute_values`: `psycopg2.extras` function for highly efficient bulk INSERT/UPDATE operations.
    * GIN and B-tree Indexes: Created on relevant columns (`normalized_address`, `address`, phonetic keys, `matched_address_id`) to accelerate lookups, joins, and similarity searches.
    * Named Cursors: Used in `fallback_match.py` for counting large result sets without loading all IDs into memory.

### Blocking Strategies

Blocking strategies are used to reduce the number of pairwise comparisons required for fuzzy and phonetic matching, which would otherwise be an intractable N² problem for large datasets.

* **Fuzzy Matching (`src/match.py`):**
    * **Strategy:** Prefix Blocking. Addresses are blocked based on the first 10 characters of their `normalized_address`. Fuzzy comparison (`pg_trgm` similarity) is only performed between transaction addresses and canonical addresses that share the same 10-character prefix.
    * **Implementation:** Implemented in the SQL query using `LEFT(t.normalized_address, 10) = LEFT(ca.address, 10)` in the JOIN condition. This leverages the GIN indexes on the normalized address fields.
* **Phonetic Matching (`src/fallback_match.py`):**
    * **Strategy:** Phonetic Key Blocking. Addresses are blocked based on their Metaphone and Soundex phonetic keys. Fuzzy comparison (`rapidfuzz.fuzz.ratio`) is only performed between a transaction address and canonical addresses that share the same Metaphone *or* Soundex key.
    * **Implementation:** Phonetic keys are pre-calculated and stored in the `canonical_addresses` table. The script loads canonical addresses into in-memory dictionaries keyed by Metaphone and Soundex keys. Transactions' phonetic keys are calculated on the fly, and lookups are performed against these in-memory dictionaries to find candidate matches.

### Fallback Waterfall

The pipeline implements a sequential matching process, attempting more precise (and typically faster) methods first and falling back to less strict methods if no match is found.

1.  **Exact Matching:** Attempts a direct equality match between the `normalized_address` of a transaction and the `address` of a canonical record. (Implemented in `src/match.py`)
2.  **Fuzzy Matching (pg_trgm):** For records not matched exactly, attempts fuzzy matching using `pg_trgm` similarity functions within blocks defined by address prefixes. (Implemented in `src/match.py`)
3.  **Phonetic Matching (Jellyfish/Rapidfuzz):** For records not matched by exact or fuzzy `pg_trgm` methods, attempts matching using phonetic keys (Metaphone/Soundex) as a blocking mechanism, followed by `rapidfuzz.fuzz.ratio` as a tie-breaker. (Implemented in `src/fallback_match.py`)
4.  **(Missing) External API Fallback:** The current implementation does not include a fallback step to an external address validation API as mentioned in the requirements. This would be the next logical step in the waterfall for records that remain unmatched after the phonetic stage.

Each matching step updates the `transactions` table with the `matched_address_id`, `match_type`, `confidence_score`, and clears the `unmatch_reason` if a match is found. If a step fails to find a match, the `unmatch_reason` is updated accordingly.

## Assumptions

* Input CSV files (`11211 Addresses.csv`, `transactions_2_11211.csv`) exist in the `./data/input` directory and have the expected column headers and data formats as defined in the ingestion scripts and schema.
* A PostgreSQL database server is available and accessible with appropriate credentials (managed via Docker Compose and `.env`).
* The `postgis` and `pg_trgm` extensions can be created in the target PostgreSQL database (requires superuser privileges).
* The `id` column in the original `transactions_2_11211.csv` is sufficient for generating unique IDs in the simulated data by appending a suffix.
* The `usaddress` library and regex patterns are sufficient for parsing the variety of raw address formats present in the transaction data.
* The chosen matching thresholds (`FUZZY_MATCH_THRESHOLD`, `PHONETIC_MATCH_CONFIDENCE`, `PHONETIC_TIEBREAK_THRESHOLD`) are appropriate for the desired balance between precision and recall.
* Memory is sufficient to load all canonical addresses into memory for the phonetic matching step (this might need re-evaluation for extremely large canonical lists).

## Optional (Extra Credit)

*(This section is not implemented in the provided code but outlines how the optional requirements could be addressed)*

* **Simple REST Endpoint:** A new service could be added to the `docker-compose.yml` using a framework like Flask or FastAPI. This service would expose an endpoint (e.g., `/match_address`) that accepts a raw address string via a POST request. The endpoint's logic would replicate the parsing and matching waterfall for the single input address and return the best match (or unmatched status) as a JSON response. This would require the API service to have database access.
* **Match Accuracy Report/Dashboard:** If ground-truth data (a version of the transaction data with known correct matches) were available, a script could be written to compare the pipeline's output (`output.csv`) against the ground truth. This script could calculate metrics like precision, recall, and accuracy per match type and store them in the database or generate a report CSV. A simple dashboard could then visualize these metrics using a library like Dash or connecting a BI tool to the database.