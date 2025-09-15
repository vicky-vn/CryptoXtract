# CryptoXtract

A containerized ETL pipeline that extracts cryptocurrency market data from the CoinGecko API, transforms it using dbt, and orchestrates workflows with Apache Airflow—all backed by PostgreSQL.

## Architecture

- **Data Source**: CoinGecko API for real-time cryptocurrency market data
- **Orchestration**: Apache Airflow for workflow management
- **Database**: PostgreSQL for data storage
- **Transformation**: dbt (Data Build Tool) for data modeling
- **Containerization**: Docker & Docker Compose for deployment

## Tech Stack

- **Language**: Python
- **Key Libraries**: pandas, requests, SQLAlchemy, psycopg2-binary
- **Infrastructure**: Docker, PostgreSQL, Apache Airflow, dbt

## Quick Start

1. **Clone the repository**
   ```bash
   git clone https://github.com/vicky-vn/CryptoXtract.git
   cd CryptoXtract
   ```

2. **Set up environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. **Start services**
   ```bash
   docker-compose up -d postgres
   ```

4. **Test connection and run**
   ```bash
   python test_connection.py
   python src/main.py
   ```

## Project Structure

```
CryptoXtract/
├── airflow/          # Airflow DAGs and configuration
├── dbt/              # dbt models and transformation configs
├── src/              # Core Python ETL scripts
├── sql/init/         # Database initialization scripts
└── docker-compose.yml
```
