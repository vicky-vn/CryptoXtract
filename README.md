# CoinGecko ETL Pipeline

A complete ETL pipeline that extracts cryptocurrency data from CoinGecko API, transforms it using dbt, and orchestrates the workflow with Apache Airflow.

## Tech Stack

- **Data Source**: CoinGecko API
- **Database**: PostgreSQL
- **Transformation**: dbt (Data Build Tool)
- **Orchestration**: Apache Airflow
- **Containerization**: Docker

## Project Structure

coingecko-etl/
├── airflow/ # Airflow DAGs and configuration
├── dbt/ # dbt models and transformations
├── src/ # Python source code
├── sql/ # Database initialization scripts
└── docker/ # Docker configuration files


## Getting Started

1. Clone the repository
2. Set up virtual environment: `python -m venv venv`
3. Install dependencies: `pip install -r requirements.txt`
4. Start PostgreSQL: `docker-compose up -d postgres`
5. Test connection: `python test_connection.py`

## Development Status

- [x] Phase 1: Project Setup ✅
- [ ] Phase 2: Data Extraction
- [ ] Phase 3: Database Schema
- [ ] Phase 4: dbt Setup
- [ ] Phase 5: Airflow Configuration
- [ ] Phase 6: Testing & Validation
