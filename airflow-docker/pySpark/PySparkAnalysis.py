import os
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F, types as T

MYSQL_HOST = os.getenv("MYSQL_HOST", "local-mysql")
MYSQL_DB   = os.getenv("MYSQL_DATABASE", "RawData")
MYSQL_USER = os.getenv("MYSQL_USER", "spark")
MYSQL_PW   = os.getenv("MYSQL_PASSWORD", "sparkpw")
CHURN_INACTIVE_DAYS = int(os.getenv("CHURN_INACTIVE_DAYS", "1"))
PROCESSED_WRITE_MODE = os.getenv("PROCESSED_WRITE_MODE", "overwrite")

START = os.getenv("PROCESSED_START")
END   = os.getenv("PROCESSED_END")

URL = (
    f"jdbc:mysql://{MYSQL_HOST}:3306/{MYSQL_DB}"
    "?useSSL=false&allowPublicKeyRetrieval=true"
    "&serverTimezone=UTC&rewriteBatchedStatements=true"
)
JDBC_PROPS = {"user": MYSQL_USER,
              "password": MYSQL_PW,
              "driver": "com.mysql.cj.jdbc.Driver",
              "useServerPrepStmts": "true",
              "useCursorFetch" : "true",
              "defaultFetchSize": "1000",
              }
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

if START and END:
    raw = raw.where(F.to_date("DP_DATE").between(START, END))

daily_activity = (
    raw.groupBy(
        F.to_date("DP_DATE").alias("Date"),
        F.col("DP_MSISDN").cast(T.StringType()).alias("User"),
    )
    .agg(
        F.count("*").alias("M_Data_Count"),
        F.lit(0).cast("int").alias("M_Out_Call_Count"),
        F.lit(0).cast("int").alias("M_Out_Call_Time"),
        F.lit(0).cast("int").alias("M_Data_Sum"),
        F.lit(0).cast("int").alias("M_In_Call_Count"),
        F.lit(0).cast("int").alias("M_In_Call_Time"),
    )
)

user_activity_window = Window.partitionBy("User").orderBy("Date")

rolled = (
    daily_activity
    .withColumn("Next_Activity_Date", F.lead("Date").over(user_activity_window))
    .withColumn(
        "M_TENURE_CHURN",
        F.when(F.col("Next_Activity_Date").isNull(), F.lit(1))
        .when(F.datediff(F.col("Next_Activity_Date"), F.col("Date")) > CHURN_INACTIVE_DAYS, F.lit(1))
        .otherwise(F.lit(0))
        .cast("int")
    )
    .select(
        F.date_format("Date", "yyyy-MM-dd").alias("Date"), "User",
        "M_Out_Call_Count", "M_Out_Call_Time",
        "M_Data_Sum", "M_Data_Count",
        "M_In_Call_Count", "M_In_Call_Time",
        "M_TENURE_CHURN",
    )
)

rolled_count = rolled.count()
print("[rolled.count]", rolled_count)
print("[label distribution]")
rolled.groupBy("M_TENURE_CHURN").count().orderBy("M_TENURE_CHURN").show()
rolled.show(10, truncate=False)

if rolled_count == 0:
    raise SystemExit("[fatal] No processed rows generated from DP_CDR_Data.")

(
    rolled.write.format("jdbc")
    .mode(PROCESSED_WRITE_MODE)
    .option("url", URL)
    .option("dbtable", f"{MYSQL_DB}.Processed_Data")
    .option("user", MYSQL_USER)
    .option("password", MYSQL_PW)
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("numPartitions", "1")
    .option("truncate", "true")
    .option("sessionInitStatement", "SET @spark_tag='analysis_write'")
    .save()
)

print(f"[write] Processed_Data {PROCESSED_WRITE_MODE} done")
