#!/usr/bin/env python3
"""
Main script for testing CoinGecko data extraction
"""

import sys
import os
from datetime import datetime

# Add src to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from extractors.coingecko_extractor import CoinGeckoExtractor
from utils.db_connection import DatabaseConnection
from config.settings import Settings

def main():
    print("🚀 Starting CoinGecko Data Extraction Test")
    print("=" * 50)
    
    # Initialize components
    extractor = CoinGeckoExtractor()
    db = DatabaseConnection()
    
    # Test database connection
    print("1. Testing database connection...")
    if not db.test_connection():
        print("❌ Database connection failed. Please check your setup.")
        return
    
    # Extract cryptocurrency data
    print("2. Extracting cryptocurrency data...")
    try:
        crypto_data = extractor.get_top_cryptocurrencies(limit=10)  # Start small for testing
        print(f"   📊 Extracted {len(crypto_data)} cryptocurrency records")
        
        # Display sample data
        if crypto_data:
            sample = crypto_data[0]
            print(f"   Sample: {sample['name']} ({sample['symbol']}) - ${sample['current_price']}")
    
    except Exception as e:
        print(f"❌ Error extracting data: {e}")
        return
    
    # Insert data into database
    print("3. Inserting data into database...")
    try:
        records_inserted = db.insert_cryptocurrency_data(crypto_data)
        print(f"   ✅ Successfully inserted {records_inserted} records")
    
    except Exception as e:
        print(f"❌ Error inserting data: {e}")
        return
    
    # Test historical data extraction
    print("4. Testing historical data extraction...")
    try:
        # Get historical data for Bitcoin
        history = extractor.get_coin_history('bitcoin', days=3)
        print(f"   📈 Extracted {len(history.get('prices', []))} price points for Bitcoin")
        
        # Insert historical data
        historical_records = db.insert_historical_data([history])
        print(f"   ✅ Successfully inserted {historical_records} historical records")
    
    except Exception as e:
        print(f"❌ Error with historical data: {e}")
    
    print("=" * 50)
    print("✅ CoinGecko Data Extraction Test Complete!")

if __name__ == "__main__":
    main()
