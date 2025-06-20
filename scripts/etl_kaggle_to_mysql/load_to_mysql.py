#Import libraries needed
import pandas as pd
import pymysql
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import os

#--------------------------------------Create conexion to database-----------------------------------
url = URL.create(
    drivername="mysql+pymysql",
    username=os.getenv("MYSQL_USER", "admin"),
    password=os.getenv("MYSQL_PASSWORD", "admin"),
    host=os.getenv("MYSQL_HOST", "mysql"),
    port=int(os.getenv("MYSQL_PORT", 3306)),
    database=os.getenv("MYSQL_DB", "medical_appointment"),
)

engine = create_engine(url,pool_pre_ping=True )

#----------------------------------Create files path with os.path--------------------------------------
#Base dir path
BASE_DIR=os.path.dirname(__file__)

#slots_transformed.csv path
slots_transformed_path=os.path.join(BASE_DIR,"..","..","data","transformed","slots_transformed.csv")
slots_transformed_path=os.path.abspath(slots_transformed_path)
#status_transformed.csv path
status_transformed_path=os.path.join(BASE_DIR,"..","..","data","transformed","status_transformed.csv")
status_transformed_path=os.path.abspath(status_transformed_path)
#patients_transformed.csv path
patients_transformed_path=os.path.join(BASE_DIR,"..","..","data","transformed","patients_transformed.csv")
patients_transformed_path=os.path.abspath(patients_transformed_path)
#appointment_transformed.csv path
appointments_transformed_path=os.path.join(BASE_DIR,"..","..","data","transformed","appointments_transformed.csv")
appointments_transformed_path=os.path.abspath(appointments_transformed_path)

#------------------------------------Load CSV files into dataframes-----------------------------------------------
df_slots = pd.read_csv(slots_transformed_path)
df_status = pd.read_csv(status_transformed_path)
df_patients = pd.read_csv(patients_transformed_path)
df_appointments = pd.read_csv(appointments_transformed_path)

#----------------------------------Load dataframe into mysql database tables---------------------------------------
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