#!/bin/bash
set -e

# Wait for the MySQL server to be ready
until mysql -h "localhost" -u "root" -p"$MYSQL_ROOT_PASSWORD" -e "SHOW DATABASES;" > /dev/null 2>&1; do
  echo "Waiting for database connection..."
  sleep 5
done

mysql -h "localhost" -u "root" -p"$MYSQL_ROOT_PASSWORD" <<'EOF'
CREATE DATABASE IF NOT EXISTS RawData;
USE RawData;

CREATE TABLE IF NOT EXISTS DP_CDR_Data (
  id INT AUTO_INCREMENT PRIMARY KEY,
  DP_DATE DATE,
  DP_MSISDN BIGINT,
  DP_MOC_COUNT INT,
  DP_MOC_DURATION INT,
  DP_MTC_COUNT INT,
  DP_MTC_DURATION INT,
  DP_MOSMS_COUNT INT,
  DP_MTSMS_COUNT INT,
  DP_DATA_COUNT INT,
  DP_DATA_VOLUME BIGINT,
  PSEUDO_CHURNED INT
);

CREATE TABLE IF NOT EXISTS Processed_Data (
  id INT AUTO_INCREMENT PRIMARY KEY,
  Date DATETIME,
  User VARCHAR(255),
  M_Out_Call_Count INT,
  M_Out_Call_Time INT,
  M_Data_Sum BIGINT,
  M_Data_Count INT,
  M_In_Call_Count INT,
  M_In_Call_Time INT,
  M_TENURE_CHURN INT
);

CREATE TABLE IF NOT EXISTS model_predictions (
  id INT AUTO_INCREMENT PRIMARY KEY,
  label DOUBLE,
  prediction DOUBLE,
  probability_0 DOUBLE,
  probability_1 DOUBLE,
  Date DATETIME
);
EOF

# Load raw CSV only if table is empty
ROW_COUNT=$(mysql -N -h "localhost" -u "root" -p"$MYSQL_ROOT_PASSWORD" -e "SELECT COUNT(*) FROM RawData.DP_CDR_Data;")

if [ "$ROW_COUNT" -eq 0 ]; then
  mysql --local-infile=1 -h "localhost" -u "root" -p"$MYSQL_ROOT_PASSWORD" <<'EOF'
USE RawData;
LOAD DATA LOCAL INFILE '/var/lib/mysql-files/RawData.csv'
INTO TABLE DP_CDR_Data
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
  DP_DATE,
  DP_MSISDN,
  DP_MOC_COUNT,
  DP_MOC_DURATION,
  DP_MTC_COUNT,
  DP_MTC_DURATION,
  DP_MOSMS_COUNT,
  DP_MTSMS_COUNT,
  DP_DATA_COUNT,
  DP_DATA_VOLUME,
  PSEUDO_CHURNED
);
EOF
  echo "RawData.csv loaded into DP_CDR_Data."
else
  echo "DP_CDR_Data already contains data. Skipping CSV load."
fi