-- Dimension table for cryptocurrencies
CREATE TABLE IF NOT EXISTS analytics.dim_cryptocurrencies (
    coin_id VARCHAR(100) PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    name VARCHAR(200) NOT NULL,
    first_seen DATE,
    last_seen DATE,
    is_active BOOLEAN DEFAULT TRUE,
    category VARCHAR(100),
    description TEXT,
    website_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact table for daily cryptocurrency metrics
CREATE TABLE IF NOT EXISTS analytics.fact_daily_metrics (
    id SERIAL PRIMARY KEY,
    coin_id VARCHAR(100) NOT NULL,
    date DATE NOT NULL,
    price_usd DECIMAL(20, 8),
    market_cap_usd BIGINT,
    volume_24h_usd BIGINT,
    market_cap_rank INTEGER,
    price_change_24h DECIMAL(20, 8),
    price_change_percentage_24h DECIMAL(10, 4),
    price_change_percentage_7d DECIMAL(10, 4),
    price_change_percentage_30d DECIMAL(10, 4),
    volatility_7d DECIMAL(10, 4),
    volume_to_market_cap_ratio DECIMAL(10, 6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (coin_id) REFERENCES analytics.dim_cryptocurrencies(coin_id),
    UNIQUE(coin_id, date)
);

-- Aggregated market performance table
CREATE TABLE IF NOT EXISTS analytics.market_performance (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    total_market_cap BIGINT,
    total_volume_24h BIGINT,
    bitcoin_dominance DECIMAL(10, 4),
    ethereum_dominance DECIMAL(10, 4),
    altcoin_dominance DECIMAL(10, 4),
    active_cryptocurrencies INTEGER,
    market_cap_change_24h DECIMAL(10, 4),
    fear_greed_index INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date)
);

-- Top performers tracking
CREATE TABLE IF NOT EXISTS analytics.daily_top_performers (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    coin_id VARCHAR(100) NOT NULL,
    performance_type VARCHAR(20) NOT NULL, -- 'gainer', 'loser', 'volume'
    rank_position INTEGER NOT NULL,
    metric_value DECIMAL(20, 8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (coin_id) REFERENCES analytics.dim_cryptocurrencies(coin_id)
);

-- Comments
COMMENT ON TABLE analytics.dim_cryptocurrencies IS 'Master dimension table for cryptocurrencies';
COMMENT ON TABLE analytics.fact_daily_metrics IS 'Daily metrics and KPIs for each cryptocurrency';
COMMENT ON TABLE analytics.market_performance IS 'Overall market performance and dominance metrics';
COMMENT ON TABLE analytics.daily_top_performers IS 'Daily top gainers, losers, and volume leaders';
