from flask import Flask, request, jsonify
import pandas as pd
import numpy as np
import os
import random
from datetime import datetime, timedelta
from main import execute_sql_query
import time
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import threading
import logging

app = Flask(__name__)

def create_random_batches_with_randomized_timestamps(df, batch_size, num_batches):
    for i in range(num_batches):
        logging.debug(f"Creating batch {i + 1}/{num_batches} with size {batch_size}")
        batch_df = df.sample(n=batch_size).reset_index(drop=True)

        current_time = datetime.now()
        random_timestamps = [current_time + timedelta(seconds=random.randint(-300, 300)) for _ in range(batch_size)]
        batch_df['DP_DATE'] = random_timestamps

        for col in batch_df.select_dtypes(include=[np.number]).columns:
            if col != 'id':
                noise = np.random.normal(0, 0.1, batch_size)
                batch_df[col] = batch_df[col] * (1 + noise)

        if 'id' in batch_df.columns:
            batch_df = batch_df.drop(columns=['id'])

        batch_df = batch_df.where(pd.notnull(batch_df), None)
        insert_into_database(batch_df, "DP_CDR_Data")
        logging.info(f"Batch {i + 1}/{num_batches} inserted into table DP_CDR_Data")

def stream_data(df, batch_size=1000, num_batches=10, interval=60):
    logging.info(f"Starting streaming for {num_batches} batches with interval {interval} seconds.")
    for batch_num in range(num_batches):
        logging.debug(f"Creating batch {batch_num + 1}/{num_batches}.")
        create_random_batches_with_randomized_timestamps(df, batch_size, 1)
        logging.info(f"Batch {batch_num + 1}/{num_batches} inserted. Waiting for {interval} seconds.")
        time.sleep(interval)
    logging.info("Streaming completed. All specified batches have been inserted.")

def insert_into_database(df, table_name):
    try:
        database_url = os.getenv('DATABASE_URL')
        logging.debug(f"Database URL: {database_url}")
        engine = create_engine(database_url)
        df = df.where(pd.notnull(df), None)
        logging.debug(f"Inserting into table {table_name}. DataFrame preview:\n{df.head()}")
        df.to_sql(table_name, engine, index=False, if_exists='append')
        logging.info(f"Data successfully inserted into {table_name}. Rows inserted: {len(df)}")
    except Exception as e:
        logging.error(f"Error inserting into {table_name}: {e}")
        raise

logging.basicConfig(level=logging.DEBUG)

stream_thread = None

@app.route('/start_stream', methods=['POST'])
def start_stream():
    global stream_thread
    try:
        if stream_thread and stream_thread.is_alive():
            return jsonify({"message": "A streaming process is already running"}), 409

        logging.debug("Received request to start streaming.")
        df = execute_sql_query(query="SELECT * FROM DP_CDR_Data LIMIT 10000", database_name="RawData")
        logging.debug(f"DataFrame fetched: {df.head()}")

        batch_size = request.json.get('batch_size', 1000)
        num_batches = request.json.get('num_batches', 10)
        interval = request.json.get('interval', 60)

        logging.debug(f"Streaming config: batch_size={batch_size}, num_batches={num_batches}, interval={interval}")
        stream_thread = threading.Thread(target=stream_data, args=(df, batch_size, num_batches, interval))
        stream_thread.start()
        logging.debug("Background thread started.")
        return jsonify({"message": "Streaming started"}), 200
    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000, threaded=True)
