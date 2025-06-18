#Import libraries
import pandas as pd
import pymysql
import psycopg2
from sqlalchemy import create_engine, text


#Create conexion to medical_appointment data base
engine_mysql = create_engine("mysql+pymysql://admin:admin@mysql:3306/medical_appointment")

#Create conexion to appointment_analysis data base
engine_postgresql = create_engine("postgresql+psycopg2://admin:admin@postgre:5432/appointment_analysis")

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

#Save extract data into CSV files
df_slots_raw.to_csv("slots_raw.csv", index=False)
df_status_raw.to_csv("status_raw.csv", index=False)
df_patients_raw.to_csv("patients_raw.csv", index=False)
df_appointment_raw.to_csv("appointment_raw.csv", index=False)

engine_mysql.dispose()
engine_postgresql.dispose()