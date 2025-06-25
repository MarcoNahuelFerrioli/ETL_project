# Medical Appointments ETL Pipeline

This project implements a complete ETL pipeline to process, transform, and load medical appointment data from CSV files (sourced from Kaggle)  
into two database systems: a transactional MySQL database and a PostgreSQL data warehouse.

## Project Structure

project/
├── data/
│   ├── raw/           # Original downloaded CSVs
│   ├── transformed/   # Cleaned CSVs ready for MySQL
│   └── extracted/     # Extracted CSVs for PostgreSQL
├── scripts/
│   ├── create_postgres_db/
│   │   └── create_datawarehouse.py
│   ├── etl_kaggle_to_mysql/
│   │   ├── load_to_mysql.py
│   │   └── transform_kaggle.py
│   └── etl_mysql_to_postgres/
│       ├── extract_mysql.py
│       ├── load_dim_date_table.py
│       └── transformAndLoad_data_to_postgresql.py
├── notebooks/
│   └── EDA.ipynb
├── docker/
│   ├── Dockerfile
│   ├── docker-compose.yml
│   └── init-scripts/
│       └── init.sql
└── README.md

## What Does This Project Do?

- 🔹 Transforms raw CSVs into normalized structures  
- 🔹 Loads transformed data into MySQL (OLTP)  
- 🔹 Incrementally extracts data from MySQL using `updated_at`  
- 🔹 Loads dimensions and facts into PostgreSQL (DW) using a star schema  
- 🔹 Generates a full `dim_date` table  
- 🔹 Uses SQLAlchemy for environment-driven DB connections  
- 🔹 Modular and reusable Python scripts  

## Technologies Used

- Python 3.11  
- pandas  
- SQLAlchemy  
- pymysql  
- psycopg2  
- MySQL  
- PostgreSQL  
- Docker  

## Main Scripts

| Script                                             | Description                                           |
|---------------------------------------------------|-------------------------------------------------------|
| `create_postgres_db/create_datawarehouse.py`      | Creates the data warehouse schemas and tables        |
| `etl_kaggle_to_mysql/transform_kaggle.py`        | Cleans and normalizes raw CSVs                        |
| `etl_kaggle_to_mysql/load_to_mysql.py`            | Loads data into MySQL transactional tables           |
| `etl_mysql_to_postgres/extract_mysql.py`          | Incrementally extracts data from MySQL to CSV        |
| `etl_mysql_to_postgres/load_dim_date_table.py`    | Generates `dim_date` in PostgreSQL                    |
| `etl_mysql_to_postgres/transformAndLoad_data_to_postgresql.py` | Loads dimensions and fact table in the data warehouse |

## Requirements

- Docker + Docker Compose  
- Python 3.11+  

## How to Run It

1. Clone the repo  

2. Build and start containers:  
```bash
docker-compose build
docker-compose up -d

The init.sql script located in docker/init-scripts/ is executed automatically when the database container initializes to create the necessary schemas and tables.

3. Run scripts in order (either inside the container or locally if you have Python and dependencies installed):
	python scripts/etl_kaggle_to_mysql/transform_kaggle.py
	python scripts/etl_kaggle_to_mysql/load_to_mysql.py
	python scripts/etl_mysql_to_postgres/extract_mysql.py
	python scripts/etl_mysql_to_postgres/load_dim_date_table.py
	python scripts/etl_mysql_to_postgres/transformAndLoad_data_to_postgresql.py


Project Status
In development — The end-to-end pipeline is functional. README and documentation are being finalized.

Marco Nahuel Ferrioli
📧 ferriolimarconahuel@gmail.com
Built for learning and professional development purposes
