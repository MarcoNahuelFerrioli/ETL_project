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