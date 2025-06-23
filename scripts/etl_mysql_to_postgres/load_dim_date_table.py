import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import os

#Create conexion to appointment_analysis data base
url = URL.create(
    drivername="postgresql+psycopg2",
    username=os.getenv("POSTGRES_USER","admin"),
    password=os.getenv("POSTGRES_PASSWORD","admin"),
    host=os.getenv("POSTGRES_HOST","postgre"),
    port=int(os.getenv("POSTGRES_PORT",5432)),
    database=os.getenv("POSTGRES_DB","appointment_analysis"),
)

engine = create_engine(url,pool_pre_ping=True)

#Generate the date range
dates = pd.date_range("2000-01-01", "2035-12-31", freq="D")
df = pd.DataFrame({
    "date_key": dates.strftime("%Y%m%d").astype(int),
    "date": dates,
    "day_of_week": dates.day_name(),  
    "month_name": dates.month_name(),  
    "quarter": dates.quarter,
    "year": dates.year,
    "is_weekend": dates.dayofweek.isin([5, 6])     
})

#Load data into dim_date
with engine.begin() as conn:
    df.to_sql("dim_date", conn, if_exists="append", index=False)

print("dim_date table loaded successfully")