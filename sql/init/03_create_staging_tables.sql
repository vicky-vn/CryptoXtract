-- Staging table for cleaned cryptocurrency data
CREATE TABLE IF NOT EXISTS staging.cryptocurrencies (
    coin_id VARCHAR(100) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    name VARCHAR(200) NOT NULL,
    current_price DECIMAL(20, 8),
    market_cap BIGINT,
    market_cap_rank INTEGER,
    total_volume BIGINT,
    price_change_24h DECIMAL(20, 8),
    price_change_percentage_24h DECIMAL(10, 4),
    high_24h DECIMAL(20, 8),
    low_24h DECIMAL(20, 8),
    circulating_supply BIGINT,
    total_supply BIGINT,
    max_supply BIGINT,
    ath DECIMAL(20, 8),
    ath_date DATE,
    atl DECIMAL(20, 8),
    atl_date DATE,
    last_updated TIMESTAMP,
    extraction_date DATE NOT NULL,
    is_valid BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Staging table for daily price aggregations
CREATE TABLE IF NOT EXISTS staging.daily_prices (
    id SERIAL PRIMARY KEY,
    coin_id VARCHAR(100) NOT NULL,
    date DATE NOT NULL,
    open_price DECIMAL(20, 8),
    high_price DECIMAL(20, 8),
    low_price DECIMAL(20, 8),
    close_price DECIMAL(20, 8),
    volume BIGINT,
    market_cap BIGINT,
    price_change DECIMAL(20, 8),
    price_change_percentage DECIMAL(10, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(coin_id, date)
);

-- Comments
COMMENT ON TABLE staging.cryptocurrencies IS 'Cleaned and validated cryptocurrency data';
COMMENT ON TABLE staging.daily_prices IS 'Daily OHLCV price data aggregated from historical data';
