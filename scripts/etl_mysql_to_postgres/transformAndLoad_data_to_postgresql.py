# Import libraries
import pandas as pd
import pymysql
import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import os

# -----------------------------Create data bases conexion---------------------------------------
# Create conexion to appointment_analysis data base
url_postgresql = URL.create(
    drivername="postgresql+psycopg2",
    username=os.getenv("POSTGRES_USER","admin"),
    password=os.getenv("POSTGRES_PASSWORD","admin"),
    host=os.getenv("POSTGRES_HOST","postgre"),
    port=int(os.getenv("POSTGRES_PORT",5432)),
    database=os.getenv("POSTGRES_DB","appointment_analysis"),
)

engine_postgresql = create_engine(url_postgresql,pool_pre_ping=True)


# ----------------------------------Create files path with os.path--------------------------------------
# Base dir path
BASE_DIR=os.path.dirname(__file__)

# slots_extracted.csv path
slots_extracted_path=os.path.join(BASE_DIR,"..","..","data","extracted","slots_extracted.csv")
slots_extracted_path=os.path.abspath(slots_extracted_path)
# status_extracted.csv path
status_extracted_path=os.path.join(BASE_DIR,"..","..","data","extracted","status_extracted.csv")
status_extracted_path=os.path.abspath(status_extracted_path)
# patients_extracted.csv path
patients_extracted_path=os.path.join(BASE_DIR,"..","..","data","extracted","patients_extracted.csv")
patients_extracted_path=os.path.abspath(patients_extracted_path)
# appointment_extracted.csv path
appointments_extracted_path=os.path.join(BASE_DIR,"..","..","data","extracted","appointments_extracted.csv")
appointments_extracted_path=os.path.abspath(appointments_extracted_path)

# ---------------------------------Load CSVs to transform------------------------------------------------
# The load_csv function was created to standardize the process of loading CSV files.
# In the next step, we use this function to create the DataFrames that will be transformed.
# Create load_csv funtion
def load_csv(file_path):
    try: 
        df = pd.read_csv(file_path)
        print(f"{file_path} file uploaded successfully")
        return df
    except FileNotFoundError:
        print(f"Error: The file {file_path} was not found.")
        return None
    except pd.errors.EmptyDataError:
        print(f"Error: The file '{file_path}' is empty.")
        return None
    except pd.errors.ParserError:
        print(f"Error: The file '{file_path}' contains corrupt or malformed data.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred while loading '{file_path}': {e}")
        return None

# Create the dataframe that will be transformed.
df_appointments = load_csv(appointments_extracted_path)
df_status = load_csv(status_extracted_path)
df_patients = load_csv(patients_extracted_path)
df_slots = load_csv(slots_extracted_path)

# ----------------------- Add date_key column into dim_slots table-------------------------------------
# Create 'date_key' from 'appointment_date' in 'df_slots' using YYYYMMDD format.
# This allows assigning the correct 'date_key' to fact_appointments without querying dim_date,
# optimizing performance and simplifying the ETL process.
df_slots['appointment_date'] = pd.to_datetime(df_slots['appointment_date'])
df_slots['date_key'] = df_slots['appointment_date'].dt.strftime("%Y%m%d").astype(int)

# ---------------# Convert 'is_available' column from 0/1 integers to boolean False/True------------------------
df_slots['is_available'] = df_slots['is_available'].astype(bool)


# ------------------Load df_status, df_patients and df_slots into apointments_analysis database (postgresql)---------

# Convert the DataFrame to a dictionary format to load data into the database using SQLAlchemy.
df_status = df_status[['status_id', 'status_description']].to_dict(orient='records')
df_patients = df_patients[['patient_id', 'name', 'sex', 'dob', 'insurance']].to_dict(orient="records")
df_slots = df_slots[['slot_id', 'appointment_date', 'appointment_time', 'is_available', 'date_key']].to_dict(orient="records")

#Writte query to insert or update data into database
query_status = text("""
INSERT INTO dim_status (status_id, status_description)
VALUES (:status_id, :status_description)
ON CONFLICT (status_id)
DO UPDATE SET
status_description = EXCLUDED.status_description;
""")

query_patients = text("""
INSERT INTO dim_patients (patient_id, name, sex, dob, insurance)
VALUES (:patient_id, :name, :sex, :dob, :insurance)
ON CONFLICT (patient_id)
DO UPDATE SET
name = EXCLUDED.name,
sex = EXCLUDED.sex,
dob = EXCLUDED.dob,
insurance = EXCLUDED.insurance;
""")

query_slots = text("""
INSERT INTO dim_slots (slot_id, appointment_date, appointment_time, is_available, date_key)
VALUES (:slot_id, :appointment_date, :appointment_time, :is_available, :date_key)
ON CONFLICT (slot_id)
DO UPDATE SET
appointment_date = EXCLUDED.appointment_date,
appointment_time = EXCLUDED.appointment_time,
is_available = EXCLUDED.is_available,
date_key = EXCLUDED.date_key;
""")

#If the DataFrame exists, load the data into the database.
if df_status:
    with engine_postgresql.begin() as conn:
        conn.execute(query_status, df_status)
        print("Status dimension loaded.")
else:
    print("No new or changed status → nothing to load.")

if df_patients:
    with engine_postgresql.begin() as conn:
        conn.execute(query_patients, df_patients)
        print("Patients dimension loaded.")
else:
    print("No new or changed patients → nothing to load.")

if df_slots:
    with engine_postgresql.begin() as conn:
        conn.execute(query_slots, df_slots)
        print("Slots dimension loaded.")
else:
    print("No new or changed slots → nothing to load.")

# --------------Extract the key and ID columns from each dimension table to load the corresponding key into fact_appointments.------------------
#Queries to extract data from dim tables
query_extract_status = text("""
SELECT status_id, status_key FROM dim_status;
""")

query_extract_patients = text("""
SELECT patient_id, patient_key FROM dim_patients;
""")

query_extract_slots = text("""
SELECT slot_id, slot_key, date_key FROM dim_slots
""")

#Execute queries to extract data
df_status_extracted = pd.read_sql(query_extract_status, engine_postgresql)
df_patients_extracted = pd.read_sql(query_extract_patients, engine_postgresql)
df_slots_extracted = pd.read_sql(query_extract_slots, engine_postgresql)

# -----------------Join df_appointments with df_status_extracted, df_patients_extracted, and df_slots_extracted. ------------------------------------
df_appointments = pd.merge(df_appointments, df_status_extracted, on="status_id")
df_appointments = pd.merge(df_appointments, df_patients_extracted, on="patient_id")
df_appointments = pd.merge(df_appointments, df_slots_extracted, on="slot_id")


# ----------------Convert time columns and replace NaN with None so PostgreSQL accepts NULLs------------
time_cols = ['check_in_time', 'start_time', 'end_time']

for col in time_cols:
    df_appointments[col] = pd.to_datetime(df_appointments[col], format="%H:%M:%S", errors="coerce").dt.time
    df_appointments[col] = df_appointments[col].where(pd.notnull(df_appointments[col]), None)



# --------------------------------------------------Rename and reorganice columns in df_appointments.---------------------------------------------------
df_appointments = df_appointments.rename(columns={
    "appointment_duration": "duration_minutes",
    "waiting_time": "waiting_minutes"
    })

df_appointments = df_appointments[['patient_key', 'status_key', 'date_key', 'slot_key', 'appointment_id', 'patient_id', 'status_id', 'slot_id', 'scheduling_date', 'check_in_time', 'start_time', 'end_time', 'waiting_minutes', 'duration_minutes']]

# ---------------------------------------------------Load data into fact_appointmets--------------------------------------
# Convert the DataFrame to a dictionary format to load data into the database using SQLAlchemy.
df_appointments = df_appointments[['patient_key', 'status_key', 'date_key', 'slot_key', 'appointment_id', 'patient_id', 'status_id', 'slot_id', 'scheduling_date', 'check_in_time', 'start_time', 'end_time', 'waiting_minutes', 'duration_minutes']].to_dict(orient='records')

#Writte query to insert or update data into database
query_appointments = text("""
INSERT INTO fact_appointments (patient_key, status_key, date_key, slot_key, appointment_id, patient_id, status_id, slot_id, scheduling_date, check_in_time, start_time, end_time, waiting_minutes, duration_minutes)
VALUES (:patient_key, :status_key, :date_key, :slot_key, :appointment_id, :patient_id, :status_id, :slot_id, :scheduling_date, :check_in_time, :start_time, :end_time, :waiting_minutes, :duration_minutes)
ON CONFLICT (appointment_id)
DO UPDATE SET
patient_key = EXCLUDED.patient_key,
status_key = EXCLUDED.status_key,
date_key = EXCLUDED.date_key,
slot_key = EXCLUDED.slot_key,
patient_id = EXCLUDED.patient_id,
status_id = EXCLUDED.status_id,
slot_id = EXCLUDED.slot_id,
scheduling_date = EXCLUDED.scheduling_date,
check_in_time = EXCLUDED.check_in_time,
start_time = EXCLUDED.start_time,
end_time = EXCLUDED.end_time,
waiting_minutes = EXCLUDED.waiting_minutes,
duration_minutes = EXCLUDED.duration_minutes;
""")
#If the DataFrame exists, load the data into the database.
if df_appointments:
    with engine_postgresql.begin() as conn:
        conn.execute(query_appointments, df_appointments)
        print("Appointments fact loaded.")
else:
    print("No new or changed appointments → nothing to load.")


