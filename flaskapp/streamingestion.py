from flask import Flask, request, jsonify
import pandas as pd
import numpy as np
import os
import random
from datetime import datetime, timedelta
from main import execute_sql_query
import time
from sqlalchemy import create_engine
import time
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import threading
app = Flask(__name__)



def create_random_batches_with_randomized_timestamps(df, batch_size, num_batches):
    for i in range(num_batches):
        batch_df = df.sample(n=batch_size).reset_index(drop=True)

        # Generate random timestamps for DP_DATE
        current_time = datetime.now()
        random_timestamps = [current_time + timedelta(seconds=random.randint(-300, 300)) for _ in range(batch_size)]
        batch_df['DP_DATE'] = random_timestamps

        # Introduce random variations in numerical columns
        for col in batch_df.select_dtypes(include=[np.number]).columns:
            if col != 'id':  # Skip modifying the id column
                noise = np.random.normal(0, 0.1, batch_size)
                batch_df[col] = batch_df[col] * (1 + noise)

        # Remove the `id` column so the database auto-generates it
        if 'id' in batch_df.columns:
            batch_df = batch_df.drop(columns=['id'])

        # Insert into the database
        insert_into_database(batch_df, "DP_CDR_Data")
        print(f"Batch {i + 1} inserted into database table DP_CDR_Data")


def stream_data(df, batch_size=1000, num_batches=10, interval=60):
    """Streams data by generating random batches at regular intervals."""
    while True:
        create_random_batches_with_randomized_timestamps(df, batch_size, num_batches)

        # Wait for the specified interval before generating the next batch
        print(f"Waiting for {interval} seconds before the next batch...")
        time.sleep(interval)


def insert_into_database(df, table_name):
    """Inserts a DataFrame into a local database table."""
    database_url = os.getenv('DATABASE_URL')
    print(f"Database URL: {database_url}")
    engine = create_engine(database_url)
    df.to_sql(table_name, engine, index=False, if_exists='append')
    print(f"Data inserted into {table_name}")

def test_stream_data(df, batch_size=1000, num_batches=1, interval=60):
    """Test function to generate one batch and avoid infinite loop."""
    create_random_batches_with_randomized_timestamps(df, batch_size, num_batches)


@app.route('/start_stream', methods=['POST'])
@app.route('/start_stream', methods=['POST'])
def start_stream():
    try:
        print("Received request to start streaming.")

        # Fetch data
        df = execute_sql_query(query="SELECT * FROM DP_CDR_Data LIMIT 10000", database_name="RawData")
        print("DataFrame fetched from SQL:")
        print(df.head())

        # Ensure 'DP_DATE' is in datetime format
        df['DP_DATE'] = pd.to_datetime(df['DP_DATE'], errors='coerce')
        print("DataFrame after converting DP_DATE:")
        print(df.head())

        # Get parameters
        batch_size = request.json.get('batch_size', 1000)
        num_batches = request.json.get('num_batches', 10)
        interval = request.json.get('interval', 60)

        print(f"Starting stream with batch_size={batch_size}, num_batches={num_batches}, interval={interval}")

        # Run streaming in a background thread
        threading.Thread(target=stream_data, args=(df, batch_size, num_batches, interval)).start()

        return jsonify({"message": "Streaming started"}), 200
    except Exception as e:
        print(f"An error occurred: {e}")
        return jsonify({"error": str(e)}), 500


# Run the Flask app
if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000, threaded=True)
