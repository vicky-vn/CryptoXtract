from src.utils.db_connection import DatabaseConnection
from datetime import datetime

def test_schema():
    db = DatabaseConnection()
    
    # Test connection
    if not db.test_connection():
        print("❌ Connection failed")
        return
    
    # Test schemas exist
    schemas_query = """
    SELECT schema_name 
    FROM information_schema.schemata 
    WHERE schema_name IN ('raw_data', 'staging', 'analytics', 'dbt_analytics')
    ORDER BY schema_name;
    """
    
    schemas = db.execute_query(schemas_query)
    print("✅ Available schemas:")
    print(schemas)
    
    # Test tables exist
    tables_query = """
    SELECT table_schema, table_name 
    FROM information_schema.tables 
    WHERE table_schema IN ('raw_data', 'staging', 'analytics')
    ORDER BY table_schema, table_name;
    """
    
    tables = db.execute_query(tables_query)
    print("\n✅ Available tables:")
    print(tables)
    
    # Test the new insert functionality
    print("\n🧪 Testing new schema-aware insertion...")
    test_data = [{
        'id': 'bitcoin',
        'symbol': 'BTC',
        'name': 'Bitcoin',
        'current_price': 45000.0,
        'market_cap': 850000000000,
        'market_cap_rank': 1,
        'roi': {'times': 100, 'currency': 'usd'},
        'extracted_at': '2024-01-01T12:00:00'
    }]
    
    try:
        records = db.insert_cryptocurrency_data(test_data, 'raw_data.cryptocurrency_data')
        print(f"✅ Successfully inserted {records} test record")
        
        # Log the extraction with proper timestamps
        now = datetime.now()
        db.insert_extraction_log(
            extraction_type='test',
            status='success',
            records_extracted=1,
            records_inserted=1,
            start_time=now,
            end_time=now,
            duration_seconds=1
        )
        print("✅ Successfully logged extraction")
        
    except Exception as e:
        print(f"❌ Error testing insertion: {e}")

if __name__ == "__main__":
    test_schema()
