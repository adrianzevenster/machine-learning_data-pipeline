import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
import os
from pyspark.sql import Row, DataFrame, functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from time import gmtime, strftime
# import numpy as np
import datetime
import sys
import matplotlib.pyplot as plt
print('Python %s on %s' % (sys.version, sys.platform))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format
from pyspark.sql.types import StringType, IntegerType, DoubleType

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format
from pyspark.sql.types import StringType, IntegerType, DoubleType


class MYSQLDataProcessor:
    def __init__(self, app_name, jdbc_url, jdbc_driver_path, user, password):
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
'''
SQL Query to Retrieve Data
'''
app_name = "MySQL PySpark DataProcessor"
jdbc_url = "jdbc:mysql://localhost:3306/RawData?useSSL=false&serverTimezone=UTC"
jdbc_driver_path = '/home/adrian/.config/JetBrains/PyCharm2024.1/jdbc-drivers/MySQL ConnectorJ/8.2.0/com/mysql/mysql-connector-j/8.2.0/mysql-connector-j-8.2.0.jar'
user = "root"
password = "a?xBVq1!"


app_name = "MySQL PySpark DataProcessor"
jdbc_url = "jdbc:mysql://localhost:3306/RawData?useSSL=false&serverTimezone=UTC"
jdbc_driver_path = '/home/adrian/.config/JetBrains/PyCharm2024.1/jdbc-drivers/MySQL ConnectorJ/8.2.0/com/mysql/mysql-connector-j/8.2.0/mysql-connector-j-8.2.0.jar'
user = "root"
password = "a?xBVq1!"

# spark = SparkSession \
#     .builder \
#     .appName("MySQL Integration") \
#     .config("spark.jars", '/home/adrian/.config/JetBrains/PyCharm2024.1/jdbc-drivers/MySQL ConnectorJ/8.2.0/com/mysql/mysql-connector-j/8.2.0/mysql-connector-j-8.2.0.jar')\
#     .getOrCreate()
#
#
#
# jdbc_url = "jdbc:mysql://localhost:3306/RawData?useSSL=false&serverTimezone=UTC"
# jdbc_connectionion = {
#     "user": "root",
#     "password": "a?xBVq1!",
#     "driver": "com.mysql.cj.jdbc.Driver"
# }

processor = MYSQLDataProcessor(app_name, jdbc_url, jdbc_driver_path, user, password)
query = "(SELECT  * from RawData.DP_CDR_Data limit 1000000) as t"

df = processor.load_data(query)
transformed_df = processor.transfrom_data(df)

transformed_df.show()

# # spark = SparkSession.builder\

# #     .appName("Data Ingestion")\
# #     .getOrCreate()
# # print(spark)
# '''Woriking'''
# # spark = SparkSession \
# #     .builder \
# #     .appName("MySQL Integration") \
# #     .config("spark.jars", '/home/adrian/.config/JetBrains/PyCharm2024.1/jdbc-drivers/MySQL ConnectorJ/8.2.0/com/mysql/mysql-connector-j/8.2.0/mysql-connector-j-8.2.0.jar')\
# #     .getOrCreate()
#
#
# # jdbc_url = "jdbc:mysql://localhost:3306/RawData?useSSL=false"
# # jdbc_connectionion = {
# #     "user" :"root",
# #     "password" : "a?xBVq1!",
# #     "driver": "com.mysql.jdbc.Driver"
# # }
# """ Testing"""
#
# '''
# SQL Query to Retrieve Data
# '''
#
# spark = SparkSession \
#     .builder \
#     .appName("MySQL Integration") \
#     .config("spark.jars", '/home/adrian/.config/JetBrains/PyCharm2024.1/jdbc-drivers/MySQL ConnectorJ/8.2.0/com/mysql/mysql-connector-j/8.2.0/mysql-connector-j-8.2.0.jar')\
#     .getOrCreate()
#
#
# jdbc_url = "jdbc:mysql://localhost:3306/RawData?useSSL=false&serverTimezone=UTC"
# jdbc_connectionion = {
#     "user": "root",
#     "password": "a?xBVq1!",
#     "driver": "com.mysql.cj.jdbc.Driver"
# }
#
# '''Unifier'''
# query = "(SELECT  * from RawData.DP_CDR_Data limit 100) as t"
#
#
# df = spark.read.jdbc(url=jdbc_url, table=query, properties=jdbc_connectionion)
#
# # Show the data (for example purposes)
# df.show()dframe
#
# # Stop the Spark session
# # spark.stop()
#
# # jdbc_url = 'jdbc:mysql://localhost:3306/RawData'
# # connection_properties = {}
# #
# # df1 = spark.read.csv('DE_Project/rawData/DP_CDR_AllUsers_April_2022.csv.gz',
# #                        inferSchema=True,
# #                        header=True)
# #
# # df1.show()
#
# # df = df.withColumn("")
# def spark_to_dataframe(df):
#
#     postDf = df.withColumn('Date', F.date_format(col('DP_DATE'), 'yyyy-MM-dd')) \
#         .withColumn('User', col('DP_MSISDN').cast(StringType())) \
#         .withColumn('M_Out_Call_Count', col('DP_MOC_COUNT').cast(IntegerType())) \
#         .withColumn('M_Out_Call_Time', col('DP_MOC_Duration').cast(DoubleType())) \
#         .withColumn('M_Data_Sum', col('DP_DATA_VOLUME').cast(DoubleType())) \
#         .withColumn('M_Data_Count', col('DP_DATA_COUNT').cast(IntegerType())) \
#         .withColumn('M_In_Call_Count', col('DP_MTC_COUNT').cast(IntegerType())) \
#         .withColumn('M_In_Call_Time', col('DP_MTC_DURATION').cast(DoubleType())) \
#         .withColumn('M_Tenure_Churn', col('PSEUDO_CHURNED').cast(IntegerType()))
#
#     return spark.createDataFrame(postDf)
#     # finalDf = df.select(
#     #     date_format(col('DP_DATE'), 'yyyy-MM-dd').alias('Date'), # Date
#     #
#     #     col('DP_MSISDN').cast(StringType()).alias('User'), # Subscriber Identifier
#     #
#     #     col('DP_MOC_COUNT').cast(IntegerType()).alias('M_Out_Call_Count'), # Outgoing Call Count
#     #     col('DP_MOC_Duration').cast(DoubleType()).alias('M_Out_Call_Time'), # Outgoing Call Duration
#     #     col('DP_DATA_VOLUME').cast(DoubleType()).alias('M_Data_Sum'), # Total Data Usage (MB)
#     #     col('DP_DATA_COUNT').cast(IntegerType()).alias('M_Data_Count'), # Total Data Purchases
#     #     col('DP_MTC_COUNT').cast(IntegerType()).alias('M_In_Call_Count'), # Incoming Call Count
#     #     col('DP_MTC_DURATION').cast(DoubleType()).alias('M_In_Call_Time'), # Incoming Call Duration
#     #
#     #     )col('PSEUDO_CHURNED').cast(IntegerType()).alias('M_Tenure_Churn') # Total Months on Network
# eda_df = spark_to_dataframe(df)
# plt.plot(eda_df['M_Data_Sum'], eda_df["M_Data_Count"])
# plt.show()
# # def eda_analysis(eda_data):
# #     summary_stats = eda_data.describe()
# #
# #     plt.plot(eda_data['M_Data_Sum'], eda_data["M_Data_Count"])
# #     plt.show()
# # preDf.show()
# # finalDf.show()
# # finalDf.printSchema()
# # print(pyspark.__version__)
# #
# # func = eda_df(eda_df)
# # plt.show()
# # DP_DATE --> Date
# # DP_MSISDN --> User
# # DP_MOC_COUNT --> Monthly_Outgoing_Calls
# # DP_MOC_DURATION --> Monthly_Call_Duration
# # DP_DATA_VOLUME --> Monthly_MB_Usage
# # DP_DATA_COUNT --> Monthly_Frequency_Data_Count
# # DP_MTC_COUNT --> Incoming Calls
# # DP_MTC_DURATION --> Incoming_Call_Duration
# # PSEUDO_CHURNED --> CHURN