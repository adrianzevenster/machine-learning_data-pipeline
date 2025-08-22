import os
import time
import datetime
import json
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, StringType

class MYSQLDataProcessor:
    def __init__(self, app_name, jdbc_url, jdbc_driver_path, user, password):
        if not os.path.isfile(jdbc_driver_path):
            raise FileNotFoundError(f"The JDBC driver jar file was not found: {jdbc_driver_path}")
        self.spark = (SparkSession.builder
                      .appName(app_name)
                      .config("spark.jars", jdbc_driver_path)
                      .config("spark.driver.memory", "4g")
                      .config("spark.executor.memory", "4g")
                      .config("spark.sql.shuffle.partitions", "8")
                      .getOrCreate())
        self.jdbc_url = jdbc_url
        self.jdbc_connection = {
            "user": user,
            "password": password,
            "driver": "com.mysql.cj.jdbc.Driver",
        }

    def load_data(self, query):
        retries, delay = 12, 10
        last_err = None
        for _ in range(retries):
            try:
                return self.spark.read.jdbc(url=self.jdbc_url, table=query, properties=self.jdbc_connection)
            except Exception as e:
                print(f"Connection failed: {e}. Retrying in {delay} seconds...")
                last_err = e
                time.sleep(delay)
        raise Exception(f"Could not connect to MySQL after several retries. Last error: {last_err}")

    def transform_data(self, df):
        # rename/convert fields as before
        postDf = df.select(
            F.date_format(col('DP_DATE'), 'yyyy-MM-dd').alias('Date'),
            col('DP_MSISDN').cast(StringType()).alias('User'),
            col('DP_MOC_COUNT').cast(IntegerType()).alias('M_Out_Call_Count'),
            col('DP_MOC_DURATION').cast(DoubleType()).alias('M_Out_Call_Time'),
            col('DP_DATA_VOLUME').cast(DoubleType()).alias('M_Data_Sum'),
            col('DP_DATA_COUNT').cast(IntegerType()).alias('M_Data_Count'),
            col('DP_MTC_COUNT').cast(IntegerType()).alias('M_In_Call_Count'),
            col('DP_MTC_DURATION').cast(DoubleType()).alias('M_In_Call_Time'),
            col('PSEUDO_CHURNED').cast(IntegerType()).alias('M_Tenure_Churn')
        )
        return postDf

    def save_data(self, df, table_name):
        df.write.jdbc(url=self.jdbc_url, table=table_name, mode='append', properties=self.jdbc_connection)
        print(f"Data written to {table_name} successfully.")

    def save_to_parquet(self, df, output_dir):
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")
        output_path = os.path.join(output_dir, f"processed_data_{timestamp}.parquet")
        df.write.mode('overwrite').parquet(output_path)
        print(f"Data written to {output_path} successfully.")

# ── Read DB params from ENV (Airflow passes these) ───────────────────────────
MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DATABASE", "RawData")

jdbc_url = f"jdbc:mysql://{MYSQL_HOST}:3306/{MYSQL_DB}?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"
jdbc_driver_path = '/opt/spark/jars/mysql-connector-java-8.0.25.jar'

processor = MYSQLDataProcessor("MySQL PySpark DataProcessor", jdbc_url, jdbc_driver_path, MYSQL_USER, MYSQL_PASSWORD)

# Config
with open('/app/config.json', 'r') as config_file:
    config = json.load(config_file)
start_date = config['start_date']
end_date = config['end_date']
query = f"(SELECT * FROM {MYSQL_DB}.DP_CDR_Data WHERE DATE(DP_DATE) BETWEEN '{start_date}' AND '{end_date}') AS t"

df = processor.load_data(query)
transformed_df = processor.transform_data(df)
transformed_df.show()

# Save to DB (you may want a separate schema/table name)
processor.save_data(transformed_df, f"{MYSQL_DB}.Processed_Data")

# Save parquet to mounted folder
parquet_output_dir = "/app/parquetFiles"
processor.save_to_parquet(transformed_df, parquet_output_dir)

