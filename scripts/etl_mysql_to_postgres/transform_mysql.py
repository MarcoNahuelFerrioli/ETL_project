#Import libraries
import pandas as pd
import os

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

#"Load CSVs to transform"
#The load_csv function was created to standardize the process of loading CSV files.
#In the next step, we use this function to create the DataFrames that will be transformed.
#Create load_csv funtion
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

#Create the dataframe that will be transformed.
df_appointments = load_csv(appointments_extracted_path)
df_status = load_csv(status_extracted_path)
df_patients = load_csv(patients_extracted_path)
df_slots = load_csv(slots_extracted_path)

#-------------------------------Transform appointments_extracted.csv--------------------------------------------------

#Drop scheduling_interval column
df_appointments.drop(["scheduling_interval"], axis=1)

#Rename columns to match with columns of appointment_analysis data base
df_appointments = df_appointments.rename(columns={
    "appointments_id":"appointment_key", 
    "slot_id":"slot_key", 
    "status_id":"status_key",
    "appointment_duration":"duration_minutes",
    "waiting_time": "waiting_minutes",
    "patient_id":"patient_key"})