import os
import mysql.connector
import pandas as pd

# Database configuration
db_config = {
    "host": os.getenv("DB_HOST", "flaskapp-db"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "RawData")
}


def table_has_data():
    """
    Checks if the target table exists and has data.
    """
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("""
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_schema = %s AND table_name = %s
        """, (db_config["database"], "DP_CDR_Data"))
        table_exists = cursor.fetchone()[0] > 0
        if table_exists:
            cursor.execute("SELECT COUNT(*) FROM DP_CDR_Data")
            row_count = cursor.fetchone()[0]
            conn.close()
            return row_count > 0
    except mysql.connector.Error as err:
        print(f"Database error: {err}")
    return False


def create_table_from_csv(csv_file):
    """
    Reads a CSV file and populates the target table.
    """
    chunksize = 10000
    try:
        for chunk in pd.read_csv(csv_file, chunksize=chunksize):
            chunk = chunk.where(pd.notnull(chunk), None)  # Replace NaN with None
            conn = mysql.connector.connect(**db_config)
            cursor = conn.cursor()

            cursor.execute("USE RawData")
            column_defs = ', '.join([f"`{col}` VARCHAR(255)" for col in chunk.columns])  # Adjust as needed
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS DP_CDR_Data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                {column_defs}
            );
            """
            cursor.execute(create_table_sql)

            placeholders = ', '.join(['%s'] * len(chunk.columns))
            insert_sql = f"INSERT INTO DP_CDR_Data ({', '.join(chunk.columns)}) VALUES ({placeholders})"
            cursor.executemany(insert_sql, chunk.where(pd.notnull(chunk), None).values.tolist())

            conn.commit()
            conn.close()
    except Exception as e:
        print(f"Error populating table: {e}")


if __name__ == "__main__":
    if table_has_data():
        print("Table already populated. Skipping database initialization.")
        exit(0)  # Exit gracefully
    else:
        print("Populating the database...")
        create_table_from_csv('/app/RawData.csv')
