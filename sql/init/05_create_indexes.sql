-- Indexes for raw_data tables
CREATE INDEX IF NOT EXISTS idx_raw_crypto_extracted_at ON raw_data.cryptocurrency_data(extracted_at DESC);
CREATE INDEX IF NOT EXISTS idx_raw_crypto_coin_id ON raw_data.cryptocurrency_data(id);
CREATE INDEX IF NOT EXISTS idx_raw_crypto_rank ON raw_data.cryptocurrency_data(market_cap_rank);

CREATE INDEX IF NOT EXISTS idx_raw_historical_coin_timestamp ON raw_data.historical_data(coin_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_raw_historical_extracted_at ON raw_data.historical_data(extracted_at DESC);

-- Indexes for staging tables
CREATE INDEX IF NOT EXISTS idx_staging_crypto_extraction_date ON staging.cryptocurrencies(extraction_date DESC);
CREATE INDEX IF NOT EXISTS idx_staging_crypto_coin_id ON staging.cryptocurrencies(coin_id);
CREATE INDEX IF NOT EXISTS idx_staging_crypto_rank ON staging.cryptocurrencies(market_cap_rank);

CREATE INDEX IF NOT EXISTS idx_staging_daily_prices_date ON staging.daily_prices(date DESC);
CREATE INDEX IF NOT EXISTS idx_staging_daily_prices_coin_date ON staging.daily_prices(coin_id, date DESC);

-- Indexes for analytics tables
CREATE INDEX IF NOT EXISTS idx_fact_daily_date ON analytics.fact_daily_metrics(date DESC);
CREATE INDEX IF NOT EXISTS idx_fact_daily_coin_date ON analytics.fact_daily_metrics(coin_id, date DESC);
CREATE INDEX IF NOT EXISTS idx_fact_daily_rank ON analytics.fact_daily_metrics(market_cap_rank);

CREATE INDEX IF NOT EXISTS idx_market_performance_date ON analytics.market_performance(date DESC);
CREATE INDEX IF NOT EXISTS idx_top_performers_date_type ON analytics.daily_top_performers(date DESC, performance_type);

-- Composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_crypto_price_rank ON analytics.fact_daily_metrics(date DESC, market_cap_rank, price_usd);
CREATE INDEX IF NOT EXISTS idx_crypto_volume_analysis ON analytics.fact_daily_metrics(date, volume_24h_usd DESC) WHERE volume_24h_usd > 1000000;
