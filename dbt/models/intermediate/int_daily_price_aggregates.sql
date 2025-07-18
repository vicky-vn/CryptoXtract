{{ config(materialized='view') }}

WITH daily_aggregates AS (
    SELECT
        coin_id,
        date,
        MIN(price) as low_price,
        MAX(price) as high_price,
        AVG(price) as avg_price,
        SUM(volume) as total_volume,
        AVG(market_cap) as avg_market_cap,
        COUNT(*) as price_points
    FROM {{ ref('stg_historical_data') }}
    GROUP BY coin_id, date
),

daily_ohlc AS (
    SELECT
        coin_id,
        date,
        
        -- Window functions to get open/close prices
        FIRST_VALUE(price) OVER (
            PARTITION BY coin_id, date 
            ORDER BY timestamp ASC 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as open_price,
        LAST_VALUE(price) OVER (
            PARTITION BY coin_id, date 
            ORDER BY timestamp ASC 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as close_price
    FROM {{ ref('stg_historical_data') }}
),

combined_data AS (
    SELECT DISTINCT
        o.coin_id,
        o.date,
        a.low_price,
        a.high_price,
        a.avg_price,
        a.total_volume,
        a.avg_market_cap,
        a.price_points,
        o.open_price,
        o.close_price
    FROM daily_ohlc o
    JOIN daily_aggregates a 
        ON o.coin_id = a.coin_id AND o.date = a.date
),

final AS (
    SELECT
        coin_id,
        date,
        low_price,
        high_price,
        avg_price,
        total_volume,
        avg_market_cap,
        price_points,
        open_price,
        close_price,
        close_price - open_price as price_change,
        CASE 
            WHEN open_price > 0 
            THEN (close_price - open_price) / open_price * 100 
            ELSE NULL 
        END as price_change_percentage,
        CASE 
            WHEN avg_price > 0 
            THEN (high_price - low_price) / avg_price * 100 
            ELSE NULL 
        END as daily_volatility
    FROM combined_data
)

SELECT * FROM final
ORDER BY coin_id, date
