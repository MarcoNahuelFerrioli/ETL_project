#Import libraries
import pandas as pd

#"Load CSVs to transform"
#The load_csv function was created to standardize the process of loading CSV files.
#In the next step, we use this function to create the DataFrames that will be transformed.

# Define the file paths for the CSVs to be used with the load_csv function
appointments = "appointments.csv"
patients = "patients.csv"
slots = "slots.csv"

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
df_appointments = load_csv(appointments)
df_patients = load_csv(patients)
df_slots = load_csv(slots)


#------------------------------------------Appointments CSV Transformation------------------------------------------

#Following the Exploratory Data Analysis, this CSV will be transformed for use in a transactional database management system with MySQL, so the file needs to be normalized.
#The Appointments CSV contains NaN values in the columns check_in_time, appointment_duration, start_time, end_time, and waiting_time. However, in this case, we do not remove these NaN values because these fields will be completed later in the DBMS.

#Transformation:

#1. We remove the columns appointment_date, appointment_time, sex, age, and age_group because this information is already present in the patients and slots tables.

#2. Transform the 'status' column to follow Third Normal Form (3NF) by referencing a status ID from a separate status lookup table. Then, rename the column to 'status_id'. (At the end of the transformation, we will create the status CSV that includes the 'status_id' and 'status_description'.)

#3. Change data types to match MySQL format

#Transformation 1
#Remove columns appointment_date, appointment_time, sex, age, and age_group
df_appointments = df_appointments.drop(["appointment_date", "appointment_time", "sex", "age", "age_group"], axis=1)

#Transformation 2
#Change the values in the status column to numeric codes (did not attend = 1, attended = 2, and cancelled = 3).

df_appointments['status'] = df_appointments['status'].replace({'did not attend' : 1, 'attended' : 2, 'cancelled' : 3, 'scheduled' : 4, 'unknown' : 5})

#Rename the column status to status_id.
df_appointments.rename(columns={'status': 'status_id'}, inplace=True)

#Transformation 3
#Change data type of column 'scheduling_date' from object to datetime
df_appointments['scheduling_date'] = pd.to_datetime(df_appointments['scheduling_date'])

#Change data type of column 'status' from objetc to int64
df_appointments['status_id'] = df_appointments['status_id'].astype(int)

#Change data type of columns 'check_in_time', 'start_time' and 'end_time' from object to datetime
# When we change the datatype to time, the rows with valid values change to time,
# but the rows with NaN values change to NaT with object datatype,
# so the column keeps having object datatype because it contains two different types.
df_appointments['check_in_time'] = pd.to_datetime(df_appointments['check_in_time'], format='%H:%M:%S', errors='coerce').dt.time
df_appointments['start_time'] = pd.to_datetime(df_appointments['start_time'], format='%H:%M:%S', errors='coerce').dt.time
df_appointments['end_time'] = pd.to_datetime(df_appointments['end_time'], format='%H:%M:%S', errors='coerce').dt.time

#-----------------------------------------------------Patients CSV tranfomation------------------------------------------------------------

#On this dataframe, we only need to change the data type of the 'dob' column to match the MySQL data type.
#Change data type of column 'dob' from object to datetime
df_patients['dob'] = pd.to_datetime(df_patients['dob'])

df_patients.dtypes

#--------------------------------------------------------Slots CSV transformation----------------------------------------------------------

#On this dataframe, we need to change the data type of column 'appointment_date' and 'appointment_time' from object to datetime to match the MySQL data type
#Change data type of column 'appointment_date' from objet to datetime
df_slots['appointment_date'] = pd.to_datetime(df_slots['appointment_date'])

#Change data type of column 'appointment_time' from object to datetime
df_slots['appointment_time'] = pd.to_datetime(df_slots['appointment_time'], format='%H:%M:%S', errors='coerce').dt.time


#------------------------------------------------------------Create status CSV-----------------------------------------------------------------

#We create this CSV following the normalization of the Appointments CSV (see Transformation 2 of the Appointments CSV).
# Declare the data dictionary containing the columns and values of the DataFrame
data = {
    'status_id': [1, 2, 3, 4, 5],
    'status_description': ['did not attend', 'attended', 'cancelled', 'scheduled', 'unknown' ]
}

#Create the dataframe status
df_status = pd.DataFrame(data)


#----------------------------------------------------Download the new and transformed CSV-------------------------------------------------------
#Download Appointment CSV transformed
df_appointments.to_csv('appointments_transformed.csv', index=False)

#Download patients CSV transformed
df_patients.to_csv('patients_transformed.csv', index=False)

#Download slots CSV transformed
df_slots.to_csv('slots_transformed.csv', index=False)

#Download status CSV (new)
df_status.to_csv('status_transformed.csv', index=False)