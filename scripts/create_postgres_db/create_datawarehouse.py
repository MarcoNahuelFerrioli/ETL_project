# We create a data warehouse on PostgreSQL for data analysis

#Import libraries
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import os

#Create connection for appointment_analysis database
url = URL.create(
    drivername="postgresql+psycopg2",
    username=os.getenv("POSTGRES_USER","admin"),
    password=os.getenv("POSTGRES_PASSWORD","admin"),
    host=os.getenv("POSTGRES_HOST","postgre"),
    port=int(os.getenv("POSTGRES_PORT",5432)),
    database=os.getenv("POSTGRES_DB","appointment_analysis"),
)

engine = create_engine(url,pool_pre_ping=True)

#Query sentences
query_dim_patient = text("""
CREATE TABLE IF NOT EXISTS dim_patient(
    patient_key INT,
    sex VARCHAR(50),
    age_group VARCHAR(20),
    insurance VARCHAR(250),
    PRIMARY KEY (patient_key)
);
""")

query_dim_date = text("""
CREATE TABLE IF NOT EXISTS dim_date(
    date_key INT,
    date DATE,
    day_of_week VARCHAR(15),
    month_name VARCHAR(15),
    quarter INT,
    year INT,
    is_weekend BOOLEAN,
    PRIMARY KEY (date_key)
);
""")

query_dim_status = text("""
CREATE TABLE IF NOT EXISTS dim_status(
    status_key INT,
    status_description VARCHAR(250),
    PRIMARY KEY (status_key)
);
""")

query_dim_slot = text("""
CREATE TABLE IF NOT EXISTS dim_slot(
    slot_key INT,
    appointment_time TIME,
    is_available BOOLEAN,
    PRIMARY KEY (slot_key)
);
""")

query_fact_appointment = text("""
CREATE TABLE IF NOT EXISTS fact_appointments(
    appointment_key INT, 
    patient_key INT, 
    status_key INT, 
    date_key INT, 
    slot_key INT, 
    scheduling_date DATE, 
    check_in_time TIME, 
    start_time TIME, 
    end_time TIME, 
    waiting_minutes NUMERIC(5,2), 
    duration_minutes NUMERIC(5,2), 
    dw_load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
    dw_update_ts TIMESTAMP NULL, 
    is_current BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (appointment_key),
    CONSTRAINT fk_patient FOREIGN KEY (patient_key) REFERENCES dim_patient (patient_key),
    CONSTRAINT fk_status FOREIGN KEY (status_key) REFERENCES dim_status (status_key),
    CONSTRAINT fk_date FOREIGN KEY (date_key) REFERENCES dim_date (date_key),
    CONSTRAINT fk_slot FOREIGN KEY (slot_key) REFERENCES dim_slot (slot_key)
    );
    """)

index_queries = [
    "CREATE INDEX idx_fact_patient_key ON fact_appointments(patient_key);",
    "CREATE INDEX idx_fact_status_key ON fact_appointments(status_key);",
    "CREATE INDEX idx_fact_date_key ON fact_appointments(date_key);",
    "CREATE INDEX idx_fact_slot_key ON fact_appointments(slot_key);"
]

#Execute create tables and indexes
with engine.connect() as conn:
    conn.execute(query_dim_status)
    conn.execute(query_dim_date)
    conn.execute(query_dim_patient)
    conn.execute(query_dim_slot)
    conn.execute(query_fact_appointment)
    for query in index_queries:
        conn.execute(text(query))
    conn.commit()