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
query_dim_patients = text("""
CREATE TABLE IF NOT EXISTS dim_patients(
    patient_key INT,
    sex VARCHAR(50),
    age_group VARCHAR(20),
    insurance VARCHAR(250),
    dw_load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
    dw_update_ts TIMESTAMP NULL, 
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
    dw_load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
    dw_update_ts TIMESTAMP NULL, 
    PRIMARY KEY (date_key)
);
""")

query_dim_status = text("""
CREATE TABLE IF NOT EXISTS dim_status(
    status_key INT,
    status_description VARCHAR(250),
    dw_load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
    dw_update_ts TIMESTAMP NULL, 
    PRIMARY KEY (status_key)
);
""")

query_dim_slots = text("""
CREATE TABLE IF NOT EXISTS dim_slots(
    slot_key INT,
    appointment_time TIME,
    is_available BOOLEAN,
    dw_load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
    dw_update_ts TIMESTAMP NULL, 
    PRIMARY KEY (slot_key)
);
""")

query_fact_appointments = text("""
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
    PRIMARY KEY (appointment_key),
    CONSTRAINT fk_patient FOREIGN KEY (patient_key) REFERENCES dim_patients (patient_key),
    CONSTRAINT fk_status FOREIGN KEY (status_key) REFERENCES dim_status (status_key),
    CONSTRAINT fk_date FOREIGN KEY (date_key) REFERENCES dim_date (date_key),
    CONSTRAINT fk_slot FOREIGN KEY (slot_key) REFERENCES dim_slots (slot_key)
    );
    """)

index_queries = [
    "CREATE INDEX idx_fact_patient_key ON fact_appointments(patient_key);",
    "CREATE INDEX idx_fact_status_key ON fact_appointments(status_key);",
    "CREATE INDEX idx_fact_date_key ON fact_appointments(date_key);",
    "CREATE INDEX idx_fact_slot_key ON fact_appointments(slot_key);",
    "CREATE INDEX idx_fact_update_ts ON fact_appointments(dw_update_ts);"

]

query_trigger_sql = text("""
CREATE OR REPLACE FUNCTION trg_set_dw_update_ts()
RETURNS TRIGGER AS $$
BEGIN
    NEW.dw_update_ts := CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS set_dw_update_ts_appointments ON fact_appointments;

CREATE TRIGGER set_dw_update_ts_appointments
BEFORE UPDATE ON fact_appointments
FOR EACH ROW
EXECUTE FUNCTION trg_set_dw_update_ts();

DROP TRIGGER IF EXISTS set_dw_update_ts_patients ON dim_patients;

CREATE TRIGGER set_dw_update_ts_patients
BEFORE UPDATE ON dim_patients
FOR EACH ROW
EXECUTE FUNCTION trg_set_dw_update_ts();

DROP TRIGGER IF EXISTS set_dw_update_ts_date ON dim_date;

CREATE TRIGGER set_dw_update_ts_date
BEFORE UPDATE ON dim_date
FOR EACH ROW
EXECUTE FUNCTION trg_set_dw_update_ts();

DROP TRIGGER IF EXISTS set_dw_update_ts_status ON dim_status;

CREATE TRIGGER set_dw_update_ts_status
BEFORE UPDATE ON dim_status
FOR EACH ROW
EXECUTE FUNCTION trg_set_dw_update_ts();

DROP TRIGGER IF EXISTS set_dw_update_ts_slots ON dim_slots;

CREATE TRIGGER set_dw_update_ts_slots
BEFORE UPDATE ON dim_slots
FOR EACH ROW
EXECUTE FUNCTION trg_set_dw_update_ts();
""")


#Execute create tables and indexes
with engine.connect() as conn:
    conn.execute(query_dim_status)
    conn.execute(query_dim_date)
    conn.execute(query_dim_patients)
    conn.execute(query_dim_slots)
    conn.execute(query_fact_appointments)
    for query in index_queries:
        conn.execute(text(query))
    conn.execute(query_trigger_sql)
    conn.commit()

print("Data warehouse tables and triggers created successfully.")
