-- Utility function to get latest extraction time
CREATE OR REPLACE FUNCTION get_latest_extraction_time()
RETURNS TIMESTAMP AS $$
BEGIN
    RETURN (SELECT MAX(extracted_at) FROM raw_data.cryptocurrency_data);
END;
$$ LANGUAGE plpgsql;

-- Function to calculate price volatility
CREATE OR REPLACE FUNCTION calculate_volatility(prices DECIMAL[])
RETURNS DECIMAL AS $$
DECLARE
    avg_price DECIMAL;
    variance DECIMAL := 0;
    price DECIMAL;
BEGIN
    -- Calculate average
    SELECT AVG(unnest) INTO avg_price FROM unnest(prices);
    
    -- Calculate variance
    FOR price IN SELECT unnest(prices) LOOP
        variance := variance + POWER(price - avg_price, 2);
    END LOOP;
    
    variance := variance / array_length(prices, 1);
    
    -- Return standard deviation as percentage
    RETURN SQRT(variance) / avg_price * 100;
END;
$$ LANGUAGE plpgsql;

-- View for latest cryptocurrency prices
CREATE OR REPLACE VIEW analytics.v_latest_prices AS
SELECT 
    d.coin_id,
    d.symbol,
    d.name,
    f.price_usd,
    f.market_cap_usd,
    f.volume_24h_usd,
    f.market_cap_rank,
    f.price_change_percentage_24h,
    f.date as last_updated
FROM analytics.dim_cryptocurrencies d
JOIN analytics.fact_daily_metrics f ON d.coin_id = f.coin_id
WHERE f.date = (SELECT MAX(date) FROM analytics.fact_daily_metrics WHERE coin_id = f.coin_id)
ORDER BY f.market_cap_rank;

-- View for top gainers/losers
CREATE OR REPLACE VIEW analytics.v_daily_movers AS
SELECT 
    date,
    coin_id,
    performance_type,
    rank_position,
    metric_value as change_percentage
FROM analytics.daily_top_performers
WHERE date = CURRENT_DATE - INTERVAL '1 day'
ORDER BY performance_type, rank_position;

-- View for market overview
CREATE OR REPLACE VIEW analytics.v_market_overview AS
SELECT 
    date,
    total_market_cap,
    total_volume_24h,
    bitcoin_dominance,
    ethereum_dominance,
    market_cap_change_24h,
    active_cryptocurrencies
FROM analytics.market_performance
ORDER BY date DESC;

-- Comments on views
COMMENT ON VIEW analytics.v_latest_prices IS 'Latest prices and market data for all cryptocurrencies';
COMMENT ON VIEW analytics.v_daily_movers IS 'Daily top gainers and losers';
COMMENT ON VIEW analytics.v_market_overview IS 'Overall market performance metrics';
