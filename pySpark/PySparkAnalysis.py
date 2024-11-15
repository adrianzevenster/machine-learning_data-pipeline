import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
import os
import time
from pyspark.sql import Row, DataFrame, functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from time import gmtime, strftime, time
import datetime
import sys
import time
import json

class MYSQLDataProcessor:
    def __init__(self, app_name, jdbc_url, jdbc_driver_path, user, password):
        if not os.path.isfile(jdbc_driver_path):
            raise FileNotFoundError(f"The JDBC driver jar file was not found: {jdbc_driver_path}")
        self.spark = SparkSession \
            .builder \
            .appName(app_name) \
            .config("spark.jars", jdbc_driver_path) \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.executor.memoryOverHead", "1g") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()

        self.jdbc_url = jdbc_url
        self.jdbc_connection = {
            "user": user,
            "password": password,
            "driver": "com.mysql.jdbc.Driver"
        }

    def load_data(self, query):
            retries = 5
            while retries > 0:
                try:
                    return self.spark.read.jdbc(url=self.jdbc_url, table=query, properties=self.jdbc_connection)
                except Exception as e:
                    print(f"Connection failed: {e}. Retrying in 10 seconds...")
                    retries -= 1
                    time.sleep(10)
            raise Exception("Could not connect to MySQL after several retries.")


    def transfrom_data(self, df):
        postDf = df.withColumn('Date', F.date_format(col('DP_DATE'), 'yyyy-MM-dd')) \
            .withColumn('User', col('DP_MSISDN').cast(StringType())) \
            .withColumn('M_Out_Call_Count', col('DP_MOC_COUNT').cast(IntegerType())) \
            .withColumn('M_Out_Call_Time', col('DP_MOC_Duration').cast(DoubleType())) \
            .withColumn('M_Data_Sum', col('DP_DATA_VOLUME').cast(DoubleType())) \
            .withColumn('M_Data_Count', col('DP_DATA_COUNT').cast(IntegerType())) \
            .withColumn('M_In_Call_Count', col('DP_MTC_COUNT').cast(IntegerType())) \
            .withColumn('M_In_Call_Time', col('DP_MTC_DURATION').cast(DoubleType())) \
            .withColumn('M_Tenure_Churn', col('PSEUDO_CHURNED').cast(IntegerType()))

        postDf = postDf.select(
            date_format(col('DP_DATE'), 'yyyy-MM-dd').alias('Date'),  # Date
            col('DP_MSISDN').cast(StringType()).alias('User'),  # Subscriber Identifier
            col('DP_MOC_COUNT').cast(IntegerType()).alias('M_Out_Call_Count'),  # Outgoing Call Count
            col('DP_MOC_Duration').cast(DoubleType()).alias('M_Out_Call_Time'),  # Outgoing Call Duration
            col('DP_DATA_VOLUME').cast(DoubleType()).alias('M_Data_Sum'),  # Total Data Usage (MB)
            col('DP_DATA_COUNT').cast(IntegerType()).alias('M_Data_Count'),  # Total Data Purchases
            col('DP_MTC_COUNT').cast(IntegerType()).alias('M_In_Call_Count'),  # Incoming Call Count
            col('DP_MTC_DURATION').cast(DoubleType()).alias('M_In_Call_Time'),  # Incoming Call Duration
            col('PSEUDO_CHURNED').cast(IntegerType()).alias('M_Tenure_Churn')  # Total Months on Network
        )

        return postDf

    '''Writing the results to Processed_Data Database'''
    def save_data(self, df, table_name):
        df.write.jdbc(url=self.jdbc_url, table=table_name, mode='append', properties=self.jdbc_connection)
        print(f"Data written to {table_name} successfully.")

    def save_to_parquet(self, df, output_dir):
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")
        output_path =  os.path.join(output_dir, F"processed_data_{timestamp}.parquet")
        # Write to parquet file
        df.write.mode('overwrite').parquet(output_path)
        print(f"Data written to {output_path} successfully.")



    def analyze_data(self, df):
        # Example analysis function that calculates some statistics
        df.groupBy('User').sum('M_Out_Call_Time').show()
        df.groupBy('Date').avg('M_Data_Sum').show()
        # Add more analysis as needed

# Initialize processor
app_name = "MySQL PySpark DataProcessor"
jdbc_url = "jdbc:mysql://flaskapp-db:3306/RawData?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"
jdbc_driver_path = '/opt/spark/jars/mysql-connector-java-8.0.25.jar'
user = "root"
password = "a?xBVq1!"

if not os.path.isfile(jdbc_driver_path):
    raise FileNotFoundError(f"The JDBC driver jar file was not found at: {jdbc_driver_path}")

processor = MYSQLDataProcessor(app_name, jdbc_url, jdbc_driver_path, user, password)

with open('/app/config.json', 'r') as config_file:
    config = json.load(config_file)

start_date = config['start_date']
end_date = config['end_date']
# Use a SQL query to filter dates within the specified range
query = f"(SELECT * FROM RawData.DP_CDR_Data WHERE DATE(DP_DATE) BETWEEN '{start_date}' AND '{end_date}') AS t"

# query = "(SELECT * FROM RawData.DP_CDR_Data WHERE DATE(DP_DATE) BETWEEN '2024-10-25' AND '2024-11-04') as t"

# Load and transform data
df = processor.load_data(query)
transformed_df = processor.transfrom_data(df)

# Show transformed data (for verification)
transformed_df.show()

# Save the transformed data to the MySQL database
processor.save_data(transformed_df, "RawData.Processed_Data")\

# Save the transformed data to parquet file
parquet_output_dir = "/app/parquetFiles"
processor.save_to_parquet(transformed_df, parquet_output_dir)
