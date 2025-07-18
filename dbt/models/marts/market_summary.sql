{{ config(materialized='table') }}

WITH daily_market_data AS (
    SELECT
        date,
        SUM(market_cap_usd) as total_market_cap,
        SUM(volume_24h_usd) as total_volume_24h,
        COUNT(*) as active_cryptocurrencies,
        AVG(price_change_percentage_24h) as avg_price_change_24h,
        
        -- Bitcoin and Ethereum dominance
        SUM(CASE WHEN coin_id = 'bitcoin' THEN market_cap_usd ELSE 0 END) as btc_market_cap,
        SUM(CASE WHEN coin_id = 'ethereum' THEN market_cap_usd ELSE 0 END) as eth_market_cap
        
    FROM {{ ref('fact_daily_metrics') }}
    WHERE market_cap_usd > 0
    GROUP BY date
),

with_dominance AS (
    SELECT
        date,
        total_market_cap,
        total_volume_24h,
        active_cryptocurrencies,
        avg_price_change_24h,
        
        -- Calculate dominance percentages
        CASE 
            WHEN total_market_cap > 0 
            THEN btc_market_cap / total_market_cap * 100 
            ELSE NULL 
        END as bitcoin_dominance,
        
        CASE 
            WHEN total_market_cap > 0 
            THEN eth_market_cap / total_market_cap * 100 
            ELSE NULL 
        END as ethereum_dominance,
        
        -- Calculate altcoin dominance
        CASE 
            WHEN total_market_cap > 0 
            THEN (total_market_cap - btc_market_cap - eth_market_cap) / total_market_cap * 100 
            ELSE NULL 
        END as altcoin_dominance
        
    FROM daily_market_data
)

SELECT 
    date,
    total_market_cap,
    total_volume_24h,
    active_cryptocurrencies,
    avg_price_change_24h,
    bitcoin_dominance,
    ethereum_dominance,
    altcoin_dominance,
    CURRENT_TIMESTAMP as created_at
FROM with_dominance
