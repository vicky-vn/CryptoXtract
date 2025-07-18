-- Create schemas for different data layers
CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS dbt_analytics;

-- Set default search path
-- ALTER DATABASE coingecko_db SET search_path = raw_data, staging, analytics, public;

COMMENT ON SCHEMA raw_data IS 'Raw data directly from APIs';
COMMENT ON SCHEMA staging IS 'Cleaned and validated data';
COMMENT ON SCHEMA analytics IS 'Business logic and aggregated data';
COMMENT ON SCHEMA dbt_analytics IS 'dbt-managed analytics tables';
