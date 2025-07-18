-- Add constraints for data quality

-- Raw data constraints
ALTER TABLE raw_data.cryptocurrency_data 
ADD CONSTRAINT chk_positive_price CHECK (current_price >= 0);

ALTER TABLE raw_data.cryptocurrency_data 
ADD CONSTRAINT chk_positive_market_cap CHECK (market_cap >= 0);

ALTER TABLE raw_data.cryptocurrency_data 
ADD CONSTRAINT chk_valid_rank CHECK (market_cap_rank > 0);

-- Staging data constraints
ALTER TABLE staging.cryptocurrencies 
ADD CONSTRAINT chk_staging_positive_price CHECK (current_price >= 0);

ALTER TABLE staging.cryptocurrencies 
ADD CONSTRAINT chk_staging_valid_rank CHECK (market_cap_rank > 0);

ALTER TABLE staging.daily_prices 
ADD CONSTRAINT chk_valid_ohlc CHECK (
    open_price >= 0 AND high_price >= 0 AND 
    low_price >= 0 AND close_price >= 0 AND
    high_price >= low_price
);

-- Analytics constraints
ALTER TABLE analytics.fact_daily_metrics 
ADD CONSTRAINT chk_analytics_positive_price CHECK (price_usd >= 0);

ALTER TABLE analytics.fact_daily_metrics 
ADD CONSTRAINT chk_analytics_valid_ratio CHECK (
    volume_to_market_cap_ratio >= 0 AND volume_to_market_cap_ratio <= 100
);

-- Dominance percentages should sum to reasonable values
ALTER TABLE analytics.market_performance 
ADD CONSTRAINT chk_dominance_values CHECK (
    bitcoin_dominance >= 0 AND bitcoin_dominance <= 100 AND
    ethereum_dominance >= 0 AND ethereum_dominance <= 100
);
