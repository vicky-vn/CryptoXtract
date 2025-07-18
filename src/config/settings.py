import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Settings:
    # Database Configuration
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'coingecko_db')
    DB_USER = os.getenv('DB_USER', 'airflow')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'airflow')
    
    # API Configuration
    COINGECKO_API_URL = os.getenv('COINGECKO_API_URL', 'https://api.coingecko.com/api/v3')
    API_RATE_LIMIT_DELAY = float(os.getenv('API_RATE_LIMIT_DELAY', '1.2'))
    
    # Extraction Configuration
    DEFAULT_CRYPTO_LIMIT = int(os.getenv('DEFAULT_CRYPTO_LIMIT', '100'))
    DEFAULT_HISTORICAL_DAYS = int(os.getenv('DEFAULT_HISTORICAL_DAYS', '7'))
    
    # Project Configuration
    PROJECT_NAME = os.getenv('PROJECT_NAME', 'coingecko-etl')
    ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')
    
    @classmethod
    def get_database_url(cls) -> str:
        """Build database connection URL"""
        return f"postgresql://{cls.DB_USER}:{cls.DB_PASSWORD}@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"
