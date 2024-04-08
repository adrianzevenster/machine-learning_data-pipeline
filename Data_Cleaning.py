import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row, DataFrame, functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from time import gmtime, strftime
# import numpy as np
import datetime
import sys
print('Python %s on %s' % (sys.version, sys.platform))
spark = SparkSession.builder\
    .appName("Data Ingestion")\
    .getOrCreate()
print(spark)
#
# df1 = spark.read.csv('DE_Project/rawData/DP_CDR_AllUsers_April_2022.csv.gz',
#                        inferSchema=True,
#                        header=True)
#
# df1.show()

df = df.withColumn("")
print(pyspark.__version__)