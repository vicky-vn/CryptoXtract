-- Raw cryptocurrency market data table
CREATE TABLE IF NOT EXISTS raw_data.cryptocurrency_data (
    id VARCHAR(100),
    symbol VARCHAR(20),
    name VARCHAR(200),
    image TEXT,
    current_price DECIMAL(20, 8),
    market_cap BIGINT,
    market_cap_rank INTEGER,
    fully_diluted_valuation BIGINT,
    total_volume BIGINT,
    high_24h DECIMAL(20, 8),
    low_24h DECIMAL(20, 8),
    price_change_24h DECIMAL(20, 8),
    price_change_percentage_24h DECIMAL(10, 4),
    market_cap_change_24h BIGINT,
    market_cap_change_percentage_24h DECIMAL(10, 4),
    circulating_supply BIGINT,
    total_supply BIGINT,
    max_supply BIGINT,
    ath DECIMAL(20, 8),
    ath_change_percentage DECIMAL(10, 4),
    ath_date TIMESTAMP,
    atl DECIMAL(20, 8),
    atl_change_percentage DECIMAL(10, 4),
    atl_date TIMESTAMP,
    roi TEXT,  -- JSON stored as text
    last_updated TIMESTAMP,
    extracted_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Raw historical price data table
CREATE TABLE IF NOT EXISTS raw_data.historical_data (
    id SERIAL PRIMARY KEY,
    coin_id VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    price DECIMAL(20, 8),
    market_cap BIGINT,
    volume BIGINT,
    extracted_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Raw global market data table
CREATE TABLE IF NOT EXISTS raw_data.global_market_data (
    id SERIAL PRIMARY KEY,
    active_cryptocurrencies INTEGER,
    upcoming_icos INTEGER,
    ongoing_icos INTEGER,
    ended_icos INTEGER,
    markets INTEGER,
    total_market_cap JSONB,
    total_volume JSONB,
    market_cap_percentage JSONB,
    market_cap_change_percentage_24h_usd DECIMAL(10, 4),
    updated_at INTEGER,
    extracted_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Data extraction log table
CREATE TABLE IF NOT EXISTS raw_data.extraction_log (
    id SERIAL PRIMARY KEY,
    extraction_type VARCHAR(50) NOT NULL, -- 'market_data', 'historical', 'global'
    status VARCHAR(20) NOT NULL, -- 'success', 'failed', 'partial'
    records_extracted INTEGER,
    records_inserted INTEGER,
    error_message TEXT,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    duration_seconds INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Comments for documentation
COMMENT ON TABLE raw_data.cryptocurrency_data IS 'Raw cryptocurrency market data from CoinGecko API';
COMMENT ON TABLE raw_data.historical_data IS 'Raw historical price and volume data';
COMMENT ON TABLE raw_data.global_market_data IS 'Raw global cryptocurrency market statistics';
COMMENT ON TABLE raw_data.extraction_log IS 'Log of all data extraction operations';
