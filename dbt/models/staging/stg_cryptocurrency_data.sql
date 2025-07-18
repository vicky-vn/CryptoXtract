{{ config(materialized='view') }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'cryptocurrency_data') }}
),

cleaned_data AS (
    SELECT
        id as coin_id,
        symbol,
        name,
        current_price,
        market_cap,
        market_cap_rank,
        fully_diluted_valuation,
        total_volume as volume_24h,  -- Create alias here
        high_24h,
        low_24h,
        price_change_24h,
        price_change_percentage_24h,
        market_cap_change_24h,
        market_cap_change_percentage_24h,
        circulating_supply,
        total_supply,
        max_supply,
        ath,
        ath_change_percentage,
        ath_date,
        atl,
        atl_change_percentage,
        atl_date,
        last_updated,
        extracted_at,
        DATE(extracted_at) as extraction_date,
        created_at,
        
        -- Add calculated fields - USE ORIGINAL COLUMN NAME
        CASE 
            WHEN market_cap > 0 THEN total_volume::DECIMAL / market_cap::DECIMAL 
            ELSE NULL 
        END as volume_to_market_cap_ratio,
        
        CASE 
            WHEN current_price > 0 AND high_24h > 0 AND low_24h > 0 
            THEN (high_24h - low_24h) / current_price * 100
            ELSE NULL 
        END as daily_volatility_percentage,
        
        -- Data quality flags
        CASE 
            WHEN current_price > 0 
            AND market_cap > 0 
            AND market_cap_rank > 0 
            AND symbol IS NOT NULL 
            AND name IS NOT NULL 
            THEN TRUE 
            ELSE FALSE 
        END as is_valid_record
        
    FROM source_data
),

final AS (
    SELECT *
    FROM cleaned_data
    WHERE extracted_at >= CURRENT_DATE - INTERVAL '30 days'  -- Only keep recent data
)

SELECT * FROM final
