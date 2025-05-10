-- Drop tables, using CASCADE to handle dependencies
-- Start by dropping tables that might have dependencies or are being removed
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS canonical_addresses CASCADE;

-- Enable the PostGIS extension if it's not already enabled
-- You need superuser privileges for this command
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_trgm;


-- Create the table for canonical addresses
-- This table will store data from 11211 Addresses.csv
CREATE TABLE canonical_addresses (
    -- Matching 'Address' headers:
    hhid VARCHAR(255), --
    fname VARCHAR(100),
    mname VARCHAR(100),
    lname VARCHAR(100),
    suffix VARCHAR(10),
    address TEXT, 
    house VARCHAR(50), 
    predir VARCHAR(10), 
    street VARCHAR(255), 
    strtype VARCHAR(50), 
    postdir VARCHAR(10), 
    apttype VARCHAR(20), 
    aptnbr VARCHAR(20), 
    city VARCHAR(100), 
    state VARCHAR(100),
    zip VARCHAR(20),
    latitude FLOAT, 
    longitude FLOAT, 
    homeownercd VARCHAR(50), 

    -- Retaining a primary key, though not explicitly in the 'Address' headers
    address_id BIGSERIAL PRIMARY KEY,

    -- Columns added by phonetic matching script (ensure they are here for initial schema)
    metaphone_key VARCHAR(255),
    soundex_key VARCHAR(4)
);

-- Create the table for transactions 
CREATE TABLE transactions (
    -- Matching 'transactions' headers:
    id VARCHAR(255) PRIMARY KEY, -- Using 'id' as primary key based on header
    status VARCHAR(50),
    price NUMERIC(15, 2),
    bedrooms NUMERIC,
    bathrooms NUMERIC,
    square_feet NUMERIC,
    address_line_1 VARCHAR(255),
    address_line_2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    zip_code VARCHAR(20),
    property_type VARCHAR(50),
    year_built NUMERIC,
    presented_by TEXT,
    brokered_by TEXT,
    presented_by_mobile VARCHAR(50),
    mls VARCHAR(100),
    listing_office_id VARCHAR(255),
    listing_agent_id VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    open_house TEXT,
    latitude FLOAT,
    longitude FLOAT,
    email VARCHAR(255),
    list_date DATE,
    pending_date DATE,
    presented_by_first_name VARCHAR(100),
    presented_by_last_name VARCHAR(100),
    presented_by_middle_name VARCHAR(100),
    presented_by_suffix VARCHAR(10),
    geog GEOMETRY(Point, 4326),

    -- Columns added by parsing/matching scripts - NOW INCLUDED IN INITIAL CREATE
    parsed_street_number VARCHAR(50),
    parsed_street_name VARCHAR(255),
    parsed_street_suffix VARCHAR(50), 
    parsed_pre_directional VARCHAR(10),
    parsed_unit VARCHAR(50), 
    parsed_zip VARCHAR(20), 
    normalized_address VARCHAR(200),
    matched_address_id BIGINT,
    match_type VARCHAR(50),
    confidence_score FLOAT,
    unmatch_reason TEXT 

);

-- Indexes for canonical_addresses table
CREATE INDEX idx_canonical_zip ON canonical_addresses (zip);
CREATE INDEX idx_canonical_city ON canonical_addresses (city);
CREATE INDEX idx_canonical_street ON canonical_addresses (street); -- Index on the street name
-- Add indexes for the phonetic keys now that they are in the CREATE TABLE statement
CREATE INDEX idx_canonical_metaphone ON canonical_addresses (metaphone_key);
CREATE INDEX idx_canonical_soundex ON canonical_addresses (soundex_key);


-- Indexes for transactions table 
CREATE INDEX idx_transactions_status ON transactions (status);
CREATE INDEX idx_transactions_price ON transactions (price);
CREATE INDEX idx_transactions_city ON transactions (city);
CREATE INDEX idx_transactions_zip_code ON transactions (zip_code);
CREATE INDEX idx_transactions_property_type ON transactions (property_type);
CREATE INDEX idx_transactions_email ON transactions (email);
CREATE INDEX idx_transactions_list_date ON transactions (list_date);
CREATE INDEX idx_transactions_pending_date ON transactions (pending_date);
-- Add indexes for raw address components 
CREATE INDEX idx_transactions_address1 ON transactions (address_line_1);
CREATE INDEX idx_transactions_address2 ON transactions (address_line_2);

-- Composite indexes for potential lookups 
CREATE INDEX idx_canonical_full_address_comp ON canonical_addresses (
    house, predir, street, strtype, postdir, apttype, aptnbr, city, state, zip
);

CREATE INDEX idx_transactions_address_comp ON transactions (
    address_line_1, address_line_2, city, state, zip_code
);

-- Indexes essential for the Python matching workflow
CREATE INDEX idx_transactions_normalized_address ON transactions (normalized_address); -- This should now succeed
CREATE INDEX idx_canonical_address ON canonical_addresses (address);
CREATE INDEX idx_transactions_matched_address_id ON transactions (matched_address_id); -- This should now succeed

-- GIN index for pg_trgm on normalized addresses for fuzzy matching
CREATE INDEX idx_transactions_normalized_address_gin_trgm
ON transactions USING GIN (normalized_address gin_trgm_ops);

-- GIN index for pg_trgm on canonical addresses for fuzzy matching step
CREATE INDEX idx_canonical_address_gin_trgm
ON canonical_addresses USING GIN (address gin_trgm_ops);


-- spatial index for the geography column in transactions (recommended for PostGIS, though not used by provided scripts)
CREATE INDEX idx_transactions_geog ON transactions USING GIST (geog);