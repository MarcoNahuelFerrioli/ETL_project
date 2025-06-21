--Use medical_appointments database
USE medical_appointments;

--Create table patient
CREATE TABLE IF NOT EXISTS patients(
    patient_id INT AUTO_INCREMENT,
    name VARCHAR(250),
    sex VARCHAR(50),
    dob DATE,
    insurance VARCHAR(250),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY(patient_id)
);

--Create table Slots
CREATE TABLE IF NOT EXISTS slots(
    slot_id INT AUTO_INCREMENT,
    appointment_date DATE,
    appointment_time TIME,
    is_available BOOLEAN,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY(slot_id)
);

--Create table status
CREATE TABLE IF NOT EXISTS status(
    status_id INT AUTO_INCREMENT,
    status_description VARCHAR(250),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY(status_id)
);

--Create table appointments
CREATE TABLE IF NOT EXISTS appointments(
    appointment_id INT AUTO_INCREMENT,
    slot_id INT,
    scheduling_date DATE,
    status_id INT, 
    check_in_time TIME,
    appointment_duration DECIMAL(4,1),
    start_time TIME,
    end_time TIME, 
    waiting_time TIME,
    patient_id INT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (appointment_id),
    CONSTRAINT fk_slot FOREIGN KEY (slot_id) REFERENCES slots (slot_id),
    CONSTRAINT fk_status FOREIGN KEY (status_id) REFERENCES status (status_id),
    CONSTRAINT fk_patient FOREIGN KEY (patient_id) REFERENCES patients (patient_id)
);

-- Create index for slots table
CREATE INDEX IF NOT EXISTS idx_slots_updated_at
    ON slots (updated_at);

-- Create index for status table
CREATE INDEX IF NOT EXISTS idx_status_updated_at
    ON status (updated_at);

-- Create index for patients table
CREATE INDEX IF NOT EXISTS idx_patients_updated_at
    ON patients (updated_at);

-- Create index for appointments table
CREATE INDEX IF NOT EXISTS idx_appointments_updated_at
    ON appointments (updated_at);

