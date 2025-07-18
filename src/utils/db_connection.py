import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text
import os
from typing import List, Dict, Optional
import logging
from dotenv import load_dotenv
import json

# Load environment variables
load_dotenv()

class DatabaseConnection:
    def __init__(self):
        self.connection_string = self._build_connection_string()
        self.engine = create_engine(
            self.connection_string,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True
        )
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def _build_connection_string(self) -> str:
        """Build PostgreSQL connection string from environment variables"""
        host = os.getenv('DB_HOST', 'localhost')
        port = os.getenv('DB_PORT', '5432')
        database = os.getenv('DB_NAME', 'coingecko_db')
        username = os.getenv('DB_USER', 'airflow')
        password = os.getenv('DB_PASSWORD', 'airflow')
        
        return f"postgresql://{username}:{password}@{host}:{port}/{database}"
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                self.logger.info("✅ Database connection successful")
                return True
        except Exception as e:
            self.logger.error(f"❌ Database connection failed: {e}")
            return False
    
    def insert_cryptocurrency_data(self, data: List[Dict], table_name: str = 'raw_data.cryptocurrency_data') -> int:
        """
        Insert cryptocurrency data into PostgreSQL with schema support
        
        Args:
            data: List of cryptocurrency data dictionaries
            table_name: Target table name with schema (e.g., 'raw_data.cryptocurrency_data')
            
        Returns:
            Number of records inserted
        """
        try:
            df = pd.DataFrame(data)
            
            # Handle JSON columns
            if 'roi' in df.columns:
                df['roi'] = df['roi'].apply(lambda x: json.dumps(x) if x else None)
            
            # Split schema and table name
            schema_table = table_name.split('.')
            if len(schema_table) == 2:
                schema, table = schema_table
            else:
                schema, table = None, table_name
            
            # Insert data using pandas to_sql with schema support
            records_inserted = df.to_sql(
                table, 
                self.engine, 
                schema=schema,
                if_exists='append', 
                index=False,
                method='multi'
            )
            
            self.logger.info(f"Successfully inserted {len(df)} records into {table_name}")
            return len(df)
            
        except Exception as e:
            self.logger.error(f"Error inserting data into {table_name}: {e}")
            raise
    
    def insert_historical_data(self, data: List[Dict], table_name: str = 'raw_data.historical_data') -> int:
        """
        Insert historical price data into PostgreSQL
        
        Args:
            data: List of historical data dictionaries
            table_name: Target table name with schema
            
        Returns:
            Number of records inserted
        """
        try:
            processed_data = []
            
            for coin_data in data:
                coin_id = coin_data['coin_id']
                extracted_at = coin_data['extracted_at']
                
                # Process price data
                if 'prices' in coin_data:
                    for timestamp, price in coin_data['prices']:
                        processed_data.append({
                            'coin_id': coin_id,
                            'timestamp': pd.to_datetime(timestamp, unit='ms'),
                            'price': price,
                            'market_cap': None,
                            'volume': None,
                            'extracted_at': extracted_at
                        })
                
                # Process market cap data
                if 'market_caps' in coin_data:
                    for i, (timestamp, market_cap) in enumerate(coin_data['market_caps']):
                        if i < len(processed_data):
                            processed_data[i]['market_cap'] = market_cap
                
                # Process volume data
                if 'total_volumes' in coin_data:
                    for i, (timestamp, volume) in enumerate(coin_data['total_volumes']):
                        if i < len(processed_data):
                            processed_data[i]['volume'] = volume
            
            if processed_data:
                df = pd.DataFrame(processed_data)
                
                # Split schema and table name
                schema_table = table_name.split('.')
                if len(schema_table) == 2:
                    schema, table = schema_table
                else:
                    schema, table = None, table_name
                
                records_inserted = df.to_sql(
                    table,
                    self.engine,
                    schema=schema,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                
                self.logger.info(f"Successfully inserted {len(df)} historical records into {table_name}")
                return len(df)
            
            return 0
            
        except Exception as e:
            self.logger.error(f"Error inserting historical data: {e}")
            raise
    
    def insert_extraction_log(self, extraction_type: str, status: str, **kwargs) -> None:
        """
        Log extraction operations
        
        Args:
            extraction_type: Type of extraction ('market_data', 'historical', 'global')
            status: Status of extraction ('success', 'failed', 'partial')
            **kwargs: Additional log parameters
        """
        try:
            log_data = {
                'extraction_type': extraction_type,
                'status': status,
                'records_extracted': kwargs.get('records_extracted'),
                'records_inserted': kwargs.get('records_inserted'),
                'error_message': kwargs.get('error_message'),
                'start_time': kwargs.get('start_time'),
                'end_time': kwargs.get('end_time'),
                'duration_seconds': kwargs.get('duration_seconds')
            }
            
            df = pd.DataFrame([log_data])
            df.to_sql('extraction_log', self.engine, schema='raw_data', if_exists='append', index=False)
            
            self.logger.info(f"Logged extraction: {extraction_type} - {status}")
            
        except Exception as e:
            self.logger.error(f"Error logging extraction: {e}")
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> Optional[pd.DataFrame]:
        """
        Execute a SQL query and return results as DataFrame
        
        Args:
            query: SQL query string
            params: Optional parameters for the query
            
        Returns:
            Query results as DataFrame or None for non-SELECT queries
        """
        try:
            with self.engine.connect() as conn:
                if params:
                    result = conn.execute(text(query), params)
                else:
                    result = conn.execute(text(query))
                
                # If it's a SELECT query, return DataFrame
                if query.strip().upper().startswith('SELECT'):
                    return pd.DataFrame(result.fetchall(), columns=result.keys())
                
                # For non-SELECT queries, commit and return None
                conn.commit()
                return None
                
        except Exception as e:
            self.logger.error(f"Error executing query: {e}")
            raise
    
    def get_latest_extraction_time(self, table_name: str = 'raw_data.cryptocurrency_data') -> Optional[str]:
        """
        Get the latest extraction timestamp from a table
        
        Args:
            table_name: Name of the table to check (with schema)
            
        Returns:
            Latest extraction timestamp or None
        """
        try:
            query = f"SELECT MAX(extracted_at) as latest_time FROM {table_name}"
            result = self.execute_query(query)
            
            if result is not None and not result.empty:
                return result.iloc[0]['latest_time']
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting latest extraction time: {e}")
            return None
    
    def get_table_row_count(self, table_name: str) -> int:
        """
        Get the number of rows in a table
        
        Args:
            table_name: Name of the table (with schema)
            
        Returns:
            Number of rows in the table
        """
        try:
            query = f"SELECT COUNT(*) as row_count FROM {table_name}"
            result = self.execute_query(query)
            
            if result is not None and not result.empty:
                return result.iloc[0]['row_count']
            
            return 0
            
        except Exception as e:
            self.logger.error(f"Error getting row count for {table_name}: {e}")
            return 0
    
    def truncate_table(self, table_name: str) -> bool:
        """
        Truncate a table (remove all data)
        
        Args:
            table_name: Name of the table to truncate (with schema)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            query = f"TRUNCATE TABLE {table_name}"
            self.execute_query(query)
            self.logger.info(f"Successfully truncated table {table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error truncating table {table_name}: {e}")
            return False
    
    def get_schema_info(self) -> pd.DataFrame:
        """
        Get information about all schemas in the database
        
        Returns:
            DataFrame with schema information
        """
        query = """
        SELECT 
            schema_name,
            COUNT(*) as table_count
        FROM information_schema.tables 
        WHERE schema_name IN ('raw_data', 'staging', 'analytics', 'dbt_analytics')
        GROUP BY schema_name
        ORDER BY schema_name
        """
        
        return self.execute_query(query)
    
    def get_table_info(self, schema_name: str = None) -> pd.DataFrame:
        """
        Get information about tables in a specific schema
        
        Args:
            schema_name: Name of schema to query (if None, returns all schemas)
            
        Returns:
            DataFrame with table information
        """
        if schema_name:
            query = """
            SELECT 
                table_schema,
                table_name,
                table_type
            FROM information_schema.tables 
            WHERE table_schema = %s
            ORDER BY table_name
            """
            return self.execute_query(query, {'schema_name_1': schema_name})
        else:
            query = """
            SELECT 
                table_schema,
                table_name,
                table_type
            FROM information_schema.tables 
            WHERE table_schema IN ('raw_data', 'staging', 'analytics', 'dbt_analytics')
            ORDER BY table_schema, table_name
            """
            return self.execute_query(query)
    
    def close_connection(self):
        """Close the database connection"""
        try:
            self.engine.dispose()
            self.logger.info("Database connection closed")
        except Exception as e:
            self.logger.error(f"Error closing database connection: {e}")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close_connection()
