import pandas as pd
import numpy as np
import os
import random
from datetime import datetime, timedelta
from main import execute_sql_query
import time
from sqlalchemy import create_engine
# READ_SQL
df = execute_sql_query(query=\
                           "SELECT * FROM RawData.DP_CDR_Data LIMIT 10000",
                       database_name=\
                       "RawData")
print(df)

# Assuming 'DP_DATE' is the name of your timestamp column and ensuring it's in datetime format
df['DP_DATE'] = pd.to_datetime(df['DP_DATE'])
print(df)


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

        # Save the batch to a CSV file (or insert into a database)
        batch_filename = f'~/1-PycharmProjects/Data_Engineering_Project/DLMSDSEDE02/Streaming/DataFiles/batch_{i + 1}.csv'
        batch_df.to_csv(batch_filename, index=False)
        print(f"Batch {i + 1} written to {batch_filename}")

        insert_into_database(batch_df, "RawData.DP_CDR_Data")
        print(f"Batch {i + 1} inserted into database table RawData.DP_CDR_Data")


def stream_data(df, batch_size=1000, num_batches=10, interval=60):
    """Streams data by generating random batches at regular intervals."""
    while True:
        create_random_batches_with_randomized_timestamps(df, batch_size, num_batches)

        # Wait for the specified interval before generating the next batch
        print(f"Waiting for {interval} seconds before the next batch...")
        time.sleep(interval)
def insert_into_database(df, database_name):
    engine = create_engine('mysql+pymysql://root:a?xBVq1!@localhost:3306/RawData')

    df.to_sql('DP_CDR_Data', engine, index=False, if_exists='append')
if __name__ == "__main__":
# Adjust these parameters as needed
    batch_size = 10000  # Number of records per batch
    num_batches = 10  # Total number of batches
    interval = 60  # Interval in seconds between each stream batch

# Start the streaming process
stream_data(df, batch_size, num_batches, interval)
