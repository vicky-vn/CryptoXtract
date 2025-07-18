import requests
import pandas as pd
import time
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class CoinGeckoExtractor:
    def __init__(self):
        self.base_url = os.getenv('COINGECKO_API_URL', 'https://api.coingecko.com/api/v3')
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'CoinGecko-ETL-Pipeline/1.0'
        })
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
    def get_top_cryptocurrencies(self, limit: int = 250) -> List[Dict]:
        """
        Fetch top cryptocurrencies by market cap
        
        Args:
            limit: Number of cryptocurrencies to fetch (max 250 per request)
            
        Returns:
            List of cryptocurrency data dictionaries
        """
        url = f"{self.base_url}/coins/markets"
        params = {
            'vs_currency': 'usd',
            'order': 'market_cap_desc',
            'per_page': min(limit, 250),  # CoinGecko API limit
            'page': 1,
            'sparkline': False,
            'price_change_percentage': '1h,24h,7d'
        }
        
        try:
            self.logger.info(f"Fetching top {limit} cryptocurrencies from CoinGecko API")
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            # Add extraction timestamp to each record
            data = response.json()
            timestamp = datetime.utcnow().isoformat()
            
            for coin in data:
                coin['extracted_at'] = timestamp
                # Handle None values that might cause issues
                coin['roi'] = coin.get('roi', {})
                
            self.logger.info(f"Successfully fetched {len(data)} cryptocurrency records")
            return data
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching cryptocurrency data: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error in get_top_cryptocurrencies: {e}")
            raise
    
    def get_coin_history(self, coin_id: str, days: int = 7) -> Dict:
        """
        Fetch historical price data for a specific coin
        
        Args:
            coin_id: CoinGecko coin ID (e.g., 'bitcoin', 'ethereum')
            days: Number of days of historical data (1-365)
            
        Returns:
            Dictionary containing price, market cap, and volume history
        """
        url = f"{self.base_url}/coins/{coin_id}/market_chart"
        params = {
            'vs_currency': 'usd',
            'days': days,
            'interval': 'daily' if days > 1 else 'hourly'
        }
        
        try:
            self.logger.info(f"Fetching {days} days of history for {coin_id}")
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            data['coin_id'] = coin_id
            data['extracted_at'] = datetime.utcnow().isoformat()
            
            return data
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching history for {coin_id}: {e}")
            raise
    
    def get_global_market_data(self) -> Dict:
        """
        Fetch global cryptocurrency market statistics
        
        Returns:
            Dictionary containing global market data
        """
        url = f"{self.base_url}/global"
        
        try:
            self.logger.info("Fetching global market data")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            data['extracted_at'] = datetime.utcnow().isoformat()
            
            return data
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching global market data: {e}")
            raise
    
    def rate_limit_delay(self, delay_seconds: float = 1.2):
        """
        Implement rate limiting for CoinGecko API
        Free tier: 10-50 calls per minute
        
        Args:
            delay_seconds: Seconds to wait between API calls
        """
        time.sleep(delay_seconds)
    
    def extract_batch_data(self, coin_ids: List[str], batch_size: int = 10) -> List[Dict]:
        """
        Extract historical data for multiple coins in batches
        
        Args:
            coin_ids: List of CoinGecko coin IDs
            batch_size: Number of coins to process in each batch
            
        Returns:
            List of historical data for all requested coins
        """
        all_data = []
        
        for i in range(0, len(coin_ids), batch_size):
            batch = coin_ids[i:i + batch_size]
            self.logger.info(f"Processing batch {i//batch_size + 1}: {len(batch)} coins")
            
            for coin_id in batch:
                try:
                    history = self.get_coin_history(coin_id, days=7)
                    all_data.append(history)
                    self.rate_limit_delay()  # Respect API rate limits
                    
                except Exception as e:
                    self.logger.error(f"Failed to fetch data for {coin_id}: {e}")
                    continue
        
        return all_data
