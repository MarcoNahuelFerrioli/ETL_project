#Import libraries
import pandas as pd
import pymysql
import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import os


#Create conexion to medical_appointment data base
url_mysql = URL.create(
    drivername="mysql+pymysql",
    username=os.getenv("MYSQL_USER", "admin"),
    password=os.getenv("MYSQL_PASSWORD", "admin"),
    host=os.getenv("MYSQL_HOST", "mysql"),
    port=int(os.getenv("MYSQL_PORT", 3306)),
    database=os.getenv("MYSQL_DB", "medical_appointment"),
)

engine_mysql = create_engine(url,pool_pre_ping=True )

#Create conexion to appointment_analysis data base
url_postgresql = URL.create(
    drivername="postgresql+psycopg2",
    username=os.getenv("POSTGRES_USER","admin"),
    password=os.getenv("POSTGRES_PASSWORD","admin"),
    host=os.getenv("POSTGRES_HOST","postgre"),
    port=int(os.getenv("POSTGRES_PORT",5432)),
    database=os.getenv("POSTGRES_DB","appointment_analysis"),
)

engine_postgresql = create_engine(url,pool_pre_ping=True)

#Query to consult the last load on fact_appointment on appointment_analysis data base
query_last_load = text("""
SELECT appointment_key
FROM   fact_appointment
WHERE  dw_load_ts = (SELECT MAX(dw_load_ts) FROM fact_appointment);""")

with engine_postgresql.connect() as pg_conn:
    last_id = pg_conn.execute(query_last_load).scalar() or 0

#Query to extract data from medical_appointment data base:
query_extract_data_slots = text("""
SELECT * FROM slots""")

query_extract_data_status = text("""
SELECT * FROM status""")

query_extract_data_patients = text("""
SELECT * FROM patients""")

query_extract_data_appointment = text("""
SELECT * FROM appointment WHERE appointment_id > :last_id""")

params={"last_id": last_id}

with engine_mysql.connect() as mysql_conn:
    df_slots_raw = pd.read_sql(text(query_extract_data_slots), mysql_conn)
    df_status_raw = pd.read_sql(text(query_extract_data_status), mysql_conn)
    df_patients_raw = pd.read_sql(text(query_extract_data_patients), mysql_conn)
    df_appointment_raw = pd.read_sql(text(query_extract_data_appointment), mysql_conn, params=params)

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