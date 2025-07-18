{{ config(materialized='view') }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'historical_data') }}
),

cleaned_data AS (
    SELECT
        coin_id,
        timestamp,
        DATE(timestamp) as date,
        price,
        market_cap,
        volume,
        extracted_at,
        
        -- Add time-based groupings
        EXTRACT(HOUR FROM timestamp) as hour,
        EXTRACT(DOW FROM timestamp) as day_of_week,
        EXTRACT(MONTH FROM timestamp) as month,
        EXTRACT(YEAR FROM timestamp) as year,
        
        -- Data quality checks
        CASE 
            WHEN price > 0 AND price IS NOT NULL THEN TRUE 
            ELSE FALSE 
        END as has_valid_price,
        
        CASE 
            WHEN volume >= 0 AND volume IS NOT NULL THEN TRUE 
            ELSE FALSE 
        END as has_valid_volume
        
    FROM source_data
),

final AS (
    SELECT *
    FROM cleaned_data
    WHERE timestamp >= CURRENT_DATE - INTERVAL '90 days'  -- Keep 90 days of history
      AND has_valid_price = TRUE
)

SELECT * FROM final
