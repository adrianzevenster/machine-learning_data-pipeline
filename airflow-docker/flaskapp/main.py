import mysql.connector
import pandas as pd
import os

def execute_sql_query(query, database_name):
    db_config = {
        "host": os.getenv("DB_HOST", "mysql"),
        "user": os.getenv("DB_USER", "spark"),
        "password": os.getenv("DB_PASSWORD", "sparkpw"),
        "database": database_name
    }
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    columns = [i[0] for i in cursor.description]
    df = pd.DataFrame(data, columns=columns)
    conn.close()
    return df
