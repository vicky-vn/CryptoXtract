{{ config(materialized='table') }}

WITH latest_daily_data AS (
    SELECT
        coin_id,
        extraction_date as date,
        current_price as price_usd,
        market_cap as market_cap_usd,
        volume_24h as volume_24h_usd,
        market_cap_rank,
        price_change_24h,
        price_change_percentage_24h,
        daily_volatility_percentage,
        volume_to_market_cap_ratio,
        
        -- Get the latest record per coin per day
        ROW_NUMBER() OVER (
            PARTITION BY coin_id, extraction_date 
            ORDER BY extracted_at DESC
        ) as rn
        
    FROM {{ ref('stg_cryptocurrency_data') }}
    WHERE is_valid_record = TRUE
),

daily_metrics AS (
    SELECT
        coin_id,
        date,
        price_usd,
        market_cap_usd,
        volume_24h_usd,
        market_cap_rank,
        price_change_24h,
        price_change_percentage_24h,
        daily_volatility_percentage,
        volume_to_market_cap_ratio
    FROM latest_daily_data
    WHERE rn = 1
),

with_additional_metrics AS (
    SELECT
        *,
        
        -- Calculate 7-day price change
        LAG(price_usd, 7) OVER (
            PARTITION BY coin_id 
            ORDER BY date
        ) as price_7d_ago,
        
        -- Calculate 30-day price change
        LAG(price_usd, 30) OVER (
            PARTITION BY coin_id 
            ORDER BY date
        ) as price_30d_ago,
        
        -- Calculate 7-day rolling volatility
        STDDEV(price_change_percentage_24h) OVER (
            PARTITION BY coin_id 
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as volatility_7d
        
    FROM daily_metrics
),

final AS (
    SELECT
        coin_id,
        date,
        price_usd,
        market_cap_usd,
        volume_24h_usd,
        market_cap_rank,
        price_change_24h,
        price_change_percentage_24h,
        
        -- Calculate multi-day percentage changes
        CASE 
            WHEN price_7d_ago > 0 
            THEN (price_usd - price_7d_ago) / price_7d_ago * 100 
            ELSE NULL 
        END as price_change_percentage_7d,
        
        CASE 
            WHEN price_30d_ago > 0 
            THEN (price_usd - price_30d_ago) / price_30d_ago * 100 
            ELSE NULL 
        END as price_change_percentage_30d,
        
        volatility_7d,
        volume_to_market_cap_ratio,
        CURRENT_TIMESTAMP as created_at
        
    FROM with_additional_metrics
)

SELECT * FROM final
