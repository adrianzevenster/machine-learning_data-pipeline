#!/usr/bin/env python3

# PySparkAnalysis.py
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T

# ---------- config ----------
MYSQL_HOST = os.getenv("MYSQL_HOST", "local-mysql")
MYSQL_DB   = os.getenv("MYSQL_DATABASE", "RawData")
MYSQL_USER = os.getenv("MYSQL_USER", "spark")
MYSQL_PW   = os.getenv("MYSQL_PASSWORD", "sparkpw")

START = os.getenv("PROCESSED_START")  # e.g. 2025-08-22
END   = os.getenv("PROCESSED_END")    # e.g. 2025-08-22

URL = (
    f"jdbc:mysql://{MYSQL_HOST}:3306/{MYSQL_DB}"
    "?sslMode=REQUIRED&enabledTLSProtocols=TLSv1.2,TLSv1.3"
    "&allowPublicKeyRetrieval=true&serverTimezone=UTC&rewriteBatchedStatements=true"
)
JDBC_PROPS = {"user": MYSQL_USER,
              "password": MYSQL_PW,
              "driver": "com.mysql.cj.jdbc.Driver",
              "useServerPrepStmts": "true",
              "useCursorFetch" : "true",
              "defaultFetchSize": "1000",
              }

# Keep it one JVM/thread to avoid classpath quirks and to use one JDBC connection for writes
spark = (
    SparkSession.builder
    .config("spark.master", "local[1]")
    .config("spark.jars", "/opt/spark/jars/mysql-connector-java-8.0.25.jar")
    .config("spark.driver.extraClassPath", "/opt/spark/jars/mysql-connector-java-8.0.25.jar")
    .config("spark.executor.extraClassPath", "/opt/spark/jars/mysql-connector-java-8.0.25.jar")
    .getOrCreate()
)


print(f"[cfg] host={MYSQL_HOST} db={MYSQL_DB} user={MYSQL_USER}")
spark.read \
    .format("jdbc") \
    .option("url", URL) \
    .option("dbtable", "(SELECT DATABASE() db, @@hostname host) t") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("user", MYSQL_USER) \
    .option("password", MYSQL_PW) \
    .option("fetchsize", "1000") \
    .load() \
    .show(truncate=False)

# ---------- read raw & rollup ----------
# Assumes raw table and columns like DP_DATE (datetime/timestamp) and DP_MSISDN (user id).
raw_tbl = "DP_CDR_Data"
raw = (spark.read
       .format("jdbc")
       .option("url", URL)
       .option("dbtable", raw_tbl)
       .option("driver", "com.mysql.cj.jdbc.Driver")
       .option("user", MYSQL_USER)
       .option("password", MYSQL_PW)
       .option("fetchsize", "1000")
       .load()
       )

# Date filter if provided
if START and END:
    raw = raw.where(F.to_date("DP_DATE").between(START, END))

# Minimal, safe rollup to the schema your model expects
# If you already compute these elsewhere, map your real aggregations here.
rolled = (
    raw.groupBy(
        F.date_format("DP_DATE", "yyyy-MM-dd").alias("Date"),
        F.col("DP_MSISDN").cast(T.StringType()).alias("User"),
    )
    .agg(
        F.count("*").alias("M_Data_Count"),
        F.lit(0).cast("int").alias("M_Out_Call_Count"),
        F.lit(0).cast("int").alias("M_Out_Call_Time"),
        F.lit(0).cast("int").alias("M_Data_Sum"),
        F.lit(0).cast("int").alias("M_In_Call_Count"),
        F.lit(0).cast("int").alias("M_In_Call_Time"),
        F.lit(0).cast("int").alias("M_TENURE_CHURN"),
    )
    .select(
        "Date", "User",
        "M_Out_Call_Count", "M_Out_Call_Time",
        "M_Data_Sum", "M_Data_Count",
        "M_In_Call_Count", "M_In_Call_Time",
        "M_TENURE_CHURN",
    )
)

print("[rolled.count]", rolled.count())
rolled.show(10, truncate=False)

# ---------- write to MySQL (append, single connection, tagged in general_log) ----------
(
    rolled.write.format("jdbc")
    .mode("append")
    .option("url", URL)
    .option("dbtable", f"{MYSQL_DB}.Processed_Data")  # fully qualified
    .option("user", MYSQL_USER)
    .option("password", MYSQL_PW)
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("numPartitions", "1")                      # single JDBC connection
    .option("sessionInitStatement", "SET @spark_tag='analysis_write'")  # visible tag
    .save()
)

print("[write] Processed_Data append done")
