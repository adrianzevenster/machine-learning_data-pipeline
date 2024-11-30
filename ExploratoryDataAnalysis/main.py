import mysql.connector
import pandas as pd


def execute_sql_query(query, database_name):
    db_config = {
        "host": "flaskapp-flaskapp-db-1",  # Database hostname
        "user": "root",  # Database username
        "password": "a?xBVq1!",  # Database password
        "database": database_name  # Database name
    }

    try:
        # Connect to the database
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute(query)

        # Fetch data and column names
        data = cursor.fetchall()
        columns = [i[0] for i in cursor.description]
        df = pd.DataFrame(data, columns=columns)

        return df

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        raise  # Re-raise the exception for debugging or logging

    finally:
        # Ensure the connection is closed
        if 'conn' in locals() and conn.is_connected():
            conn.close()

if __name__ == "__main__":
    query = "SELECT * FROM DP_CDR_Data LIMIT 5"  # Replace with a valid query
    database_name = "RawData"

    try:
        df = execute_sql_query(query, database_name)
        print(df)
    except Exception as e:
        print(f"Test failed: {e}")
