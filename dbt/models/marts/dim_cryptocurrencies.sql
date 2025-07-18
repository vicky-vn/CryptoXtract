{{ config(materialized='table') }}

WITH latest_data AS (
    SELECT
        coin_id,
        symbol,
        name,
        extraction_date,
        ROW_NUMBER() OVER (PARTITION BY coin_id ORDER BY extraction_date DESC) as rn
    FROM {{ ref('stg_cryptocurrency_data') }}
    WHERE is_valid_record = TRUE
),

cryptocurrency_info AS (
    SELECT
        coin_id,
        symbol,
        name,
        extraction_date as last_seen_date
    FROM latest_data
    WHERE rn = 1
),

first_seen AS (
    SELECT 
        coin_id,
        MIN(extraction_date) as first_seen_date
    FROM {{ ref('stg_cryptocurrency_data') }}
    WHERE is_valid_record = TRUE
    GROUP BY coin_id
),

final AS (
    SELECT
        ci.coin_id,
        ci.symbol,
        ci.name,
        fs.first_seen_date,
        ci.last_seen_date,
        CASE 
            WHEN ci.last_seen_date >= CURRENT_DATE - INTERVAL '7 days' 
            THEN TRUE 
            ELSE FALSE 
        END as is_active,
        
        -- Categorize by symbol patterns
        CASE 
            WHEN ci.symbol LIKE '%USD%' OR ci.symbol LIKE '%USDT%' OR ci.symbol LIKE '%USDC%' 
            THEN 'Stablecoin'
            WHEN ci.symbol IN ('BTC', 'ETH', 'BNB', 'ADA', 'SOL', 'DOT', 'AVAX', 'MATIC') 
            THEN 'Major Altcoin'
            WHEN ci.coin_id IN ('bitcoin', 'ethereum') 
            THEN 'Blue Chip'
            ELSE 'Other'
        END as category,
        
        CURRENT_TIMESTAMP as created_at,
        CURRENT_TIMESTAMP as updated_at
        
    FROM cryptocurrency_info ci
    LEFT JOIN first_seen fs ON ci.coin_id = fs.coin_id
)

SELECT * FROM final
