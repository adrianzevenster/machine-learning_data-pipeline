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


def create_table_from_csv(csv_file):
    chunksize = 10000  # Adjust based on your system's capabilities
    for chunk in pd.read_csv(csv_file, chunksize=chunksize):#compression='gzip'# # Add a clause to check file formats (GZIP)
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS RawData")
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


# create_table_from_csv('/home/adrian/1-PycharmProjects/Data_Engineering_Project/DLMSDSEDE02/Docker/')