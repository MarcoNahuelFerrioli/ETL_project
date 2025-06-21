#Import libraries
import pandas as pd
import pymysql
import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import os




#Create conexion to appointment_analysis data base
url_postgresql = URL.create(
    drivername="postgresql+psycopg2",
    username=os.getenv("POSTGRES_USER","admin"),
    password=os.getenv("POSTGRES_PASSWORD","admin"),
    host=os.getenv("POSTGRES_HOST","postgre"),
    port=int(os.getenv("POSTGRES_PORT",5432)),
    database=os.getenv("POSTGRES_DB","appointment_analysis"),
)

engine_postgresql = create_engine(url_postgresql,pool_pre_ping=True)

#Query to consult the last load on fact_appointment on appointment_analysis data base

query_last_ts_slots = text("""
SELECT last_extract_ts FROM dw_last_ts WHERE source_table = 'slots';
""")

query_last_ts_status = text("""
SELECT last_extract_ts FROM dw_last_ts WHERE source_table = 'status';
""")

query_last_ts_patients = text("""
SELECT last_extract_ts FROM dw_last_ts WHERE source_table = 'patients';
""")

query_last_ts_appointments = text("""
SELECT last_extract_ts FROM dw_last_ts WHERE source_table = 'appointments';
""")

with engine_postgresql.connect() as pg_conn:
    last_ts_slots = pg_conn.execute(query_last_ts_slots).scalar() or pd.Timestamp("1970-01-01")
    last_ts_status = pg_conn.execute(query_last_ts_status).scalar() or pd.Timestamp("1970-01-01")
    last_ts_patients = pg_conn.execute(query_last_ts_patients).scalar() or pd.Timestamp("1970-01-01")
    last_ts_appointments = pg_conn.execute(query_last_ts_appointments).scalar() or pd.Timestamp("1970-01-01")

#Create conexion to medical_appointment data base
url_mysql = URL.create(
    drivername="mysql+pymysql",
    username=os.getenv("MYSQL_USER", "admin"),
    password=os.getenv("MYSQL_PASSWORD", "admin"),
    host=os.getenv("MYSQL_HOST", "mysql"),
    port=int(os.getenv("MYSQL_PORT", 3306)),
    database=os.getenv("MYSQL_DB", "medical_appointments"),
)

engine_mysql = create_engine(url_mysql,pool_pre_ping=True )

#Query to extract data from medical_appointment data base:
query_extract_data_slots = text("""
SELECT * FROM slots WHERE updated_at > :last_ts_slots;""")

query_extract_data_status = text("""
SELECT * FROM status WHERE updated_at > :last_ts_status;""")

query_extract_data_patients = text("""
SELECT * FROM patients WHERE updated_at > :last_ts_patients;""")

query_extract_data_appointment = text("""
SELECT * FROM appointments WHERE updated_at > :last_ts_appointments;""")

#Declare params used in querys
tables = ["slots", "status", "patients", "appointments"]

#Create dict to save the last timestamp in the tables of mysql
last_ts_dict = {} 

#Execute querys
with engine_mysql.connect() as mysql_conn:
    df_slots_raw = pd.read_sql(query_extract_data_slots, mysql_conn, params={"last_ts_slots": last_ts_slots})
    df_status_raw = pd.read_sql(query_extract_data_status, mysql_conn, params={"last_ts_status": last_ts_status})
    df_patients_raw = pd.read_sql(query_extract_data_patients, mysql_conn, params={"last_ts_patients": last_ts_patients})
    df_appointment_raw = pd.read_sql(query_extract_data_appointment, mysql_conn, params={"last_ts_appointments": last_ts_appointments})
    for tbl in tables:
        result = mysql_conn.execute(text(f"SELECT MAX(updated_at) FROM {tbl}"))
        last_ts = result.scalar() or pd.Timestamp("1970-01-01")
        last_ts_dict[tbl] = last_ts

#-----------------------------Load last timestamp into dw_last_ts table -----------------------------------
upsert_sql = text("""
    INSERT INTO dw_last_ts (source_table, last_extract_ts)
    VALUES (:tbl, :ts)
    ON CONFLICT (source_table)
    DO UPDATE SET last_extract_ts = EXCLUDED.last_extract_ts;
""")

#Execute query to load data
with engine_postgresql.begin() as pg_conn: 
    for tbl, ts in last_ts_dict.items():
        pg_conn.execute(upsert_sql, {"tbl": tbl, "ts": ts})


#----------------------------------Create files path with os.path--------------------------------------
#Base dir path
BASE_DIR=os.path.dirname(__file__)

#slots_extracted.csv path
slots_extracted_path=os.path.join(BASE_DIR,"..","..","data","extracted","slots_extracted.csv")
slots_extracted_path=os.path.abspath(slots_extracted_path)
#status_extracted.csv path
status_extracted_path=os.path.join(BASE_DIR,"..","..","data","extracted","status_extracted.csv")
status_extracted_path=os.path.abspath(status_extracted_path)
#patients_extracted.csv path
patients_extracted_path=os.path.join(BASE_DIR,"..","..","data","extracted","patients_extracted.csv")
patients_extracted_path=os.path.abspath(patients_extracted_path)
#appointment_extracted.csv path
appointments_extracted_path=os.path.join(BASE_DIR,"..","..","data","extracted","appointments_extracted.csv")
appointments_extracted_path=os.path.abspath(appointments_extracted_path)




#Save extract data into CSV files
df_slots_raw.to_csv(slots_extracted_path, index=False)
df_status_raw.to_csv(status_extracted_path, index=False)
df_patients_raw.to_csv(patients_extracted_path, index=False)
df_appointment_raw.to_csv(appointments_extracted_path, index=False)

engine_mysql.dispose()
engine_postgresql.dispose()