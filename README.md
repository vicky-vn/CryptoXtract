# CoinGecko ETL Pipeline

# CryptoXtract
A complete ETL pipeline that extracts cryptocurrency market data from the CoinGecko API, loads it into PostgreSQL, transforms it using dbt, and orchestrates the workflow with Apache Airflow—all containerized using Docker.

## Tech Stack

Programming Language: Python
-ETL Orchestration: Apache Airflow
-Data Source: CoinGecko API
-Database: PostgreSQL
-Data Transformation: dbt (Data Build Tool)
-Containerization: Docker, Docker Compose

## Main Python Libraries
-pandas
-numpy
-requests
-SQLAlchemy
-psycopg2-binary
-python-dotenv
-pytest (testing)
-black, flake8 (code quality)

See requirements.txt for exact versions and dependencies.

## Project Structure

CryptoXtract/
│
├── airflow/           # Airflow DAGs and configuration
├── dbt/               # dbt models and transformation configs (dbt_project.yml, profiles.yml)
├── src/               # Core Python ETL scripts
├── sql/init/          # Database schema and initialization scripts
├── docker-compose.yml # Docker Compose configuration
├── requirements.txt   # Python dependencies
├── test_connection.py # Database connectivity test
├── test_schema.py     # Schema test scripts
└── README.md

Getting Started
1. Clone the Repository

git clone https://github.com/vicky-vn/CryptoXtract.git
cd CryptoXtract

2. Set Up the Python Virtual Environment
python -m venv venv
source venv/bin/activate         # On Windows: venv\\Scripts\\activate

3. Install Python Dependencies
pip install -r requirements.txt

4. Start PostgreSQL via Docker
docker-compose up -d postgres

5. Initialize the Database Schema

6. Test the Database Connection
python test_connection.py

7. Run the ETL Scripts / Airflow Orchestration
python src/main.py

