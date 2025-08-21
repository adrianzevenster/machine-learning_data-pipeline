import mysql.connector
import pandas as pd


# main.py  (inside ExploratoryDataAnalysis)
import os
import mysql.connector
import pandas as pd

def execute_sql_query(query, database_name):
    db_config = {
        "host": os.getenv("MYSQL_HOST", "mysql"),   # ← read from env
        "user": os.getenv("MYSQL_USER", "root"),
        "password": os.getenv("MYSQL_PASSWORD", ""),
        "database": database_name,
    }
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    columns = [i[0] for i in cursor.description]
    df = pd.DataFrame(data, columns=columns)
    conn.close()
    return df

if __name__ == "__main__":
    query = "SELECT * FROM DP_CDR_Data LIMIT 5"  # Replace with a valid query
    database_name = "RawData"

    try:
        df = execute_sql_query(query, database_name)
        print(df)
    except Exception as e:
        print(f"Test failed: {e}")
