--Use medical_appointments database
USE medical_appointments;

--Create table patient
CREATE TABLE IF NOT EXISTS patient(
    patient_id INT, 
    name VARCHAR(250),
    sex VARCHAR(50),
    dob DATE,
    insurance VARCHAR(250),
    PRIMARY KEY(patient_id)
);

--Create table Slots
CREATE TABLE IF NOT EXISTS slots(
    slot_id INT,
    appointment_date DATE,
    appointment_time TIME,
    is_available BOOLEAN,
    PRIMARY KEY(slot_id)
);

--Create table status
CREATE TABLE IF NOT EXISTS status(
    status_id INT,
    status_description VARCHAR(250),
    PRIMARY KEY(status_id)
);

--Create table appointments
CREATE TABLE appointments(
    appointment_id INT,
    slot_id INT,
    scheduling_date DATE,
    status_id INT, 
    check_in_time TIME,
    appointment_duration DECIMAL(4,1),
    start_time TIME,
    end_time TIME, 
    waiting_time TIME,
    patient_id INT,
    PRIMARY KEY (appointment_id),
    CONSTRAINT fk_slot FOREIGN KEY (slot_id) REFERENCES slots (slot_id),
    CONSTRAINT fk_status FOREIGN KEY (status_id) REFERENCES status (status_id),
    CONSTRAINT fk_patient FOREIGN KEY (patient_id) REFERENCES patient (patient_id)
);


