-- Drop tables, using CASCADE to handle dependencies
-- Start by dropping tables that might have dependencies or are being removed
DROP TABLE IF EXISTS transactions CASCADE; -- Explicitly drop the table you want to create/rename
DROP TABLE IF EXISTS canonical_addresses CASCADE;


-- Create the table for canonical addresses
-- This table will store data from 11211 Addresses.csv, matching the provided 'Address' headers
CREATE TABLE canonical_addresses (
    -- Matching 'Address' headers:
    hhid VARCHAR(255), -- Assuming hhid can be a string identifier
    fname VARCHAR(100),
    mname VARCHAR(100),
    lname VARCHAR(100),
    suffix VARCHAR(10),
    address TEXT, -- Assuming 'address' might be a raw string
    house VARCHAR(50), -- Matching 'house' header
    predir VARCHAR(10), -- Matching 'predir' header
    street VARCHAR(255), -- Matching 'street' header
    strtype VARCHAR(50), -- Matching 'strtype' header
    postdir VARCHAR(10), -- Matching 'postdir' header
    apttype VARCHAR(20), -- Matching 'apttype' header
    aptnbr VARCHAR(20), -- Matching 'aptnbr' header
    city VARCHAR(100), -- Matching 'city' header
    state VARCHAR(100), -- Matching 'state' header
    zip VARCHAR(20), -- Matching 'zip' header
    latitude FLOAT, -- Matching 'latitude' header
    longitude FLOAT, -- Matching 'longitude' header
    homeownercd VARCHAR(50), -- Assuming homeowner code is a string

    -- Retaining a primary key, though not explicitly in the 'Address' headers
    address_id BIGSERIAL PRIMARY KEY
);

-- Create the table for transactions (formerly property_listings)
-- This table will store data matching the 'transactions' headers (from your first list)
CREATE TABLE transactions (
    -- Matching 'transactions' headers:
    id VARCHAR(255) PRIMARY KEY, -- Using 'id' as primary key based on header
    status VARCHAR(50),
    price NUMERIC(15, 2),    -- Changed from DECIMAL to NUMERIC for better compatibility
    bedrooms NUMERIC,        -- Changed from INT to NUMERIC to handle potential large values
    bathrooms NUMERIC,       -- Changed from INT to NUMERIC to handle potential large values
    square_feet NUMERIC,     -- Changed from INT to NUMERIC to handle potential large values
    address_line_1 VARCHAR(255),
    address_line_2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    zip_code VARCHAR(20),
    property_type VARCHAR(50),
    year_built NUMERIC,      -- Changed from INT to NUMERIC to handle potential large values
    presented_by TEXT, -- Using TEXT as the format of 'presented_by' isn't specified (could be full name)
    brokered_by TEXT,  -- Using TEXT for broker information
    presented_by_mobile VARCHAR(50), -- Assuming mobile number is a string
    mls VARCHAR(100), -- Assuming MLS identifier is a string
    listing_office_id VARCHAR(255), -- Assuming office ID is a string
    listing_agent_id VARCHAR(255), -- Assuming agent ID is a string
    created_at TIMESTAMP WITH TIME ZONE, -- Assuming timestamps
    updated_at TIMESTAMP WITH TIME ZONE, -- Assuming timestamps
    open_house TEXT, -- Could be JSON data or description, using TEXT
    latitude FLOAT,
    longitude FLOAT,
    email VARCHAR(255),
    list_date DATE,
    pending_date DATE,
    presented_by_first_name VARCHAR(100), -- Matching header
    presented_by_last_name VARCHAR(100),  -- Matching header
    presented_by_middle_name VARCHAR(100), -- Matching header
    presented_by_suffix VARCHAR(10),    -- Matching header
    geog GEOMETRY(Point, 4326) -- Geospatial column for the property location (using PostGIS)
);


-- Indexes for canonical_addresses table
CREATE INDEX idx_canonical_zip ON canonical_addresses (zip);
CREATE INDEX idx_canonical_city ON canonical_addresses (city);
CREATE INDEX idx_canonical_street ON canonical_addresses (street); -- Index on the street name

-- Indexes for transactions table (formerly property_listings)
CREATE INDEX idx_transactions_status ON transactions (status);
CREATE INDEX idx_transactions_price ON transactions (price);
CREATE INDEX idx_transactions_city ON transactions (city);
CREATE INDEX idx_transactions_zip_code ON transactions (zip_code);
CREATE INDEX idx_transactions_property_type ON transactions (property_type);
CREATE INDEX idx_transactions_email ON transactions (email);
CREATE INDEX idx_transactions_list_date ON transactions (list_date);
CREATE INDEX idx_transactions_pending_date ON transactions (pending_date);
-- Add indexes for address components in transactions as they'll be used for matching lookup
CREATE INDEX idx_transactions_address1 ON transactions (address_line_1);
CREATE INDEX idx_transactions_address2 ON transactions (address_line_2);


-- Composite indexes for potential lookups
CREATE INDEX idx_canonical_full_address_comp ON canonical_addresses (
    house, predir, street, strtype, postdir, apttype, aptnbr, city, state, zip
);

CREATE INDEX idx_transactions_address_comp ON transactions (
    address_line_1, address_line_2, city, state, zip_code
);