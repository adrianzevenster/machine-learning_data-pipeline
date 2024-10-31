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
app = Flask(__name__)



def create_random_batches_with_randomized_timestamps(df, batch_size, num_batches):
    """Generates batches of data with randomized timestamps and variations in data."""
    for i in range(num_batches):
        # Randomly sample data from the original DataFrame
        batch_df = df.sample(n=batch_size).reset_index(drop=True)

        # Generate random timestamps around the current time
        current_time = datetime.now()
        random_timestamps = [current_time + timedelta(seconds=random.randint(-300, 300)) for _ in range(batch_size)]

        # Assign random timestamps to the 'DP_DATE' column
        batch_df['DP_DATE'] = random_timestamps

        # Introduce random variations in numerical data (as an example)
        for col in batch_df.select_dtypes(include=[np.number]).columns:
            noise = np.random.normal(0, 0.1, batch_size)  # Adding some noise
            batch_df[col] = batch_df[col] * (1 + noise)

        # Ensure the directory exists or create it
        directory_path = os.path.expanduser('~/1-PycharmProjects/Data_Engineering_Project/DLMSDSEDE02/DataFiles/')
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)

        # Save the batch to a CSV file in the desired directory
        batch_filename = os.path.join(directory_path, f'batch_{i + 1}.csv')
        batch_df.to_csv(batch_filename, index=False)
        print(f"Batch {i + 1} written to {batch_filename}")

        # Insert batch into the database
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
def start_stream():
    try:
        # Log that the request has been received
        print("Received request to start streaming.")

        # Fetch data
        df = execute_sql_query(query="SELECT * FROM DP_CDR_Data LIMIT 10000", database_name="RawData")
        print("DataFrame fetched from SQL:")
        print(df.head())

        # Ensure 'DP_DATE' is in datetime format
        df['DP_DATE'] = pd.to_datetime(df['DP_DATE'])
        print("DataFrame after converting DP_DATE:")
        print(df.head())

        # Get parameters from the request or use default values
        batch_size = request.json.get('batch_size', 10000)
        num_batches = request.json.get('num_batches', 10)
        interval = request.json.get('interval', 60)


        print(f"Starting stream with batch_size={batch_size}, num_batches={num_batches}, interval={interval}")

        # Test streaming function
        test_stream_data(df, batch_size, num_batches, interval)

        return jsonify({"message": "Streaming test completed"}), 200
    except Exception as e:
        print(f"An error occurred: {e}")  # This will print the actual error message
        return jsonify({"error": str(e)}), 500

# Run the Flask app
if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000, threaded=True)
