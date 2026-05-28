CREATE DATABASE IF NOT EXISTS RawData;

USE RawData;

CREATE TABLE IF NOT EXISTS Processed_Data (
    Date DATE NOT NULL,
    User VARCHAR(64) NOT NULL,
    M_Out_Call_Count INT NOT NULL DEFAULT 0,
    M_Out_Call_Time DOUBLE NOT NULL DEFAULT 0,
    M_Data_Sum DOUBLE NOT NULL DEFAULT 0,
    M_Data_Count INT NOT NULL DEFAULT 0,
    M_In_Call_Count INT NOT NULL DEFAULT 0,
    M_In_Call_Time DOUBLE NOT NULL DEFAULT 0,
    M_TENURE_CHURN INT NOT NULL,
    INDEX idx_processed_date (Date),
    INDEX idx_processed_label (M_TENURE_CHURN),
    INDEX idx_processed_user_date (User, Date)
);

CREATE TABLE IF NOT EXISTS model_predictions (
    label DOUBLE NOT NULL,
    prediction DOUBLE NOT NULL,
    probability_0 DOUBLE NOT NULL,
    probability_1 DOUBLE NOT NULL,
    Date DATETIME NOT NULL,
    INDEX idx_predictions_date (Date),
    INDEX idx_predictions_label (label)
);
