#Import libraries needed
import pandas as pd
import pymysql
from sqlalchemy import create_engine

#Create conexion to database
engine = create_engine("mysql+pymysql://admin:admin@mysql:3306/medical_appointment")

#Load CSV files into dataframes
df_slots = pd.read_csv("slots_transformed.csv")
df_status = pd.read_csv("status_transformed.csv")
df_patients = pd.read_csv("patients_transformed.csv")
df_appointments = pd.read_csv("appointments_transformed.csv")

#Load dataframe into mysql database tables
df_slots.to_sql(
    "slots",
    con=engine,
    if_exists="replace",
    index=False
)

df_status.to_sql(
    "status",
    con=engine,
    if_exists="replace",
    index=False
)

df_patients.to_sql(
    "patients",
    con=engine,
    if_exists="replace",
    index=False
)

df_appointments.to_sql(
    "appointments",
    con=engine,
    if_exists="replace",
    index=False
)