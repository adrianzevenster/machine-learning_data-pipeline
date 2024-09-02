import mysql.connector
import pandas as pd

db_config = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "a?xBVq1!",
}

'''
Using chunking for insert
'''
def infer_sql_dtype(pandas_dtype):
    if pandas_dtype == 'int64':
        return 'BIGINT'
    elif pandas_dtype == 'float64':
        return 'DOUBLE'
    elif pandas_dtype == 'object':
        return 'TEXT'
    else:
        return 'TEXT'

def database_config(rawData):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("rawData")
    conn.close()
def create_table_from_csv(csv_file):
    chunksize = 10000  # Adjust based on your system's capabilities
    for chunk in pd.read_csv(csv_file, chunksize=chunksize):  #, compression='gzip'
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        # cursor.execute("CREATE DATABASE IF NOT EXISTS RawData")
        cursor.execute("USE RawData")

        column_defs = ', '.join([f"`{col}` {infer_sql_dtype(chunk[col].dtype)}" for col in chunk.columns])
        create_table_sql = f"CREATE TABLE IF NOT EXISTS DP_CDR_Data ({column_defs})"
        cursor.execute(create_table_sql)

        placeholders = ', '.join(['%s'] * len(chunk.columns))
        insert_sql = f"INSERT INTO DP_CDR_Data VALUES ({placeholders})"

        # Adjust the cursor.executemany call
        cursor.executemany(insert_sql, chunk.values.tolist())

        conn.commit()
        conn.close()


create_table_from_csv('DataFiles/DP_CDR_AllUsers_April_2022.csv.gz')

# def bathes_data_loader()

import pandas as pd
import os

# Step 1: Read the large CSV file
df = pd.read_csv('path/to/your/large_file.csv')

# Step 2 & 3: Sample randomized data and split into batches
def create_random_batches(df, batch_size, num_batches):
    for i in range(num_batches):
        batch_df = df.sample(n=batch_size)
        batch_df.to_csv(f'batch_{i+1}.csv', index=False)

# Adjust these parameters as needed
batch_size = 1000  # Number of records per batch
num_batches = 10   # Total number of batches

create_random_batches(df, batch_size, num_batches)

# Step 5: Import Data into MySQL Database
# Note: You must replace 'your_database', 'your_table', and the file path as per your setup
# Also, ensure that MySQL server has access to the path where CSV files are saved
for i in range(num_batches):
    os.system(f"""
    mysql -u your_username -p'your_password' -e "
    LOAD DATA INFILE 'absolute/path/to/batch_{i+1}.csv'
    INTO TABLE your_database.your_table
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '\"'
    LINES TERMINATED BY '\\n'
    IGNORE 1 ROWS;"
    """)
