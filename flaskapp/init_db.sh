#!/bin/bash
# Wait for the MySQL server to be ready
until mysql -h "localhost" -u "root" -p"$MYSQL_ROOT_PASSWORD" -e "SHOW DATABASES;" > /dev/null 2>&1; do
  echo "Waiting for database connection..."
  sleep 5
done

# Create the table if it doesn't exist and load data
mysql -h "localhost" -u "root" -p"$MYSQL_ROOT_PASSWORD" <<EOF
USE RawData;
CREATE TABLE IF NOT EXISTS DP_CDR_Data (
  id INT AUTO_INCREMENT PRIMARY KEY,
  DP_DATE DATE,
  DP_MSISDN INT,
  DP_MOC_COUNT INT,
  DP_MOC_DURATION INT,
  DP_MTC_COUNT INT,
  DP_MTC_MTC_DURATION INT,
  DP_MOSMS_COUNT INT
  DP_MTSMS_COUNT INT,
  DP_DATA_COUNT INT,
  DP_DATA_VOLUME BIGINT,
  PSEUDO_CHURNED INT
  -- Add more columns based on your CSV structure
);

LOAD DATA INFILE '/var/lib/mysql-files/RawData.csv'
INTO TABLE DP_CDR_Data
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
EOF

