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
# import numpy as np
import datetime
import sys
# import matplotlib.pyplot as plt
class MYSQLDataProcessor:
    def __init__(self, app_name, jdbc_url, jdbc_driver_path, user, password):
        if not os.path.isfile(jdbc_driver_path):
            raise FileNotFoundError(f"The JDBC driver jar file was not found: {jdbc_driver_path}")
        self.spark = SparkSession \
            .builder \
            .appName(app_name) \
            .config("spark.jars", jdbc_driver_path) \
            .getOrCreate()

        self.jdbc_url = jdbc_url
        self.jdbc_connection = {
            "user": user,
            "password": password,
            "driver": "com.mysql.jdbc.Driver"
        }

    def load_data(self, query):
        return self.spark.read.jdbc(url=self.jdbc_url, table=query, properties=self.jdbc_connection)

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

            col('PSEUDO_CHURNED').cast(IntegerType()).alias('M_Tenure_Churn'))  # Total Months on Network

        return postDf

    def analyze_data(self, df):
        # Example analysis function that calculates some statistics
        df.groupBy('User').sum('M_Out_Call_Time').show()
        df.groupBy('Date').avg('M_Data_Sum').show()
        # Add more analysis as needed



app_name = "MySQL PySpark DataProcessor"
jdbc_url = "jdbc:mysql://localhost:3306/RawData?useSSL=false&serverTimezone=UTC"
jdbc_driver_path = '/home/adrian/.config/JetBrains/PyCharm2024.1/jdbc-drivers/MySQL ConnectorJ/8.2.0/com/mysql/mysql-connector-j/8.2.0/mysql-connector-j-8.2.0.jar'
user = "root"
password = "a?xBVq1!"

if not os.path.isfile(jdbc_driver_path):
        raise FileNotFoundError(f"The JDBC driver jar file was not found at: {jdbc_driver_path}")


processor = MYSQLDataProcessor(app_name, jdbc_url, jdbc_driver_path, user, password)
query = "(SELECT  * from RawData.DP_CDR_Data limit 1000000) as t"

# Measure performance of analyze_data method

df = processor.load_data(query)
transformed_df = processor.transfrom_data(df)

transformed_df.show()

