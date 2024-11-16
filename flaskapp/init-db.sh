
mysql -u root -p"$MYSQL_ROOT_PASSWORD" RawData -e "
LOAD DATA INFILE '/docker-entrypoint-initdb.d/RawData.csv.gz'
INTO TABLE DP_CDR_Data
FIELDS TERMINATED BY ','
ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;"