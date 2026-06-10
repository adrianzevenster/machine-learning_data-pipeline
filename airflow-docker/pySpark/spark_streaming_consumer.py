import os
import socket
import time

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_CDR_TOPIC", "cdr-events")
MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_DB = os.getenv("MYSQL_DATABASE", "RawData")
MYSQL_USER = os.getenv("MYSQL_USER", "spark")
MYSQL_PASS = os.getenv("MYSQL_PASSWORD", "sparkpw")
TRIGGER_SECONDS = int(os.getenv("SPARK_STREAMING_TRIGGER_SECONDS", "30"))
CHECKPOINT_DIR = os.getenv("SPARK_STREAMING_CHECKPOINT_DIR", "/tmp/spark_streaming_checkpoint")

JDBC_URL = (
    f"jdbc:mysql://{MYSQL_HOST}:3306/{MYSQL_DB}"
    "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
)

COUNT_COLS = ["DP_MOC_COUNT", "DP_MTC_COUNT", "DP_MOSMS_COUNT", "DP_MTSMS_COUNT", "DP_DATA_COUNT"]
FLOAT_COLS = ["DP_MOC_DURATION", "DP_MTC_DURATION"]

CDR_SCHEMA = StructType([
    StructField("DP_DATE", StringType(), True),
    StructField("DP_MSISDN", StringType(), True),
    StructField("DP_MOC_COUNT", IntegerType(), True),
    StructField("DP_MOC_DURATION", DoubleType(), True),
    StructField("DP_MTC_COUNT", IntegerType(), True),
    StructField("DP_MTC_DURATION", DoubleType(), True),
    StructField("DP_MOSMS_COUNT", IntegerType(), True),
    StructField("DP_MTSMS_COUNT", IntegerType(), True),
    StructField("DP_DATA_COUNT", IntegerType(), True),
    StructField("DP_DATA_VOLUME", DoubleType(), True),
    StructField("PSEUDO_CHURNED", IntegerType(), True),
])


def _wait_for_host(host, port, attempts=20, delay=3):
    for i in range(1, attempts + 1):
        try:
            with socket.create_connection((host, port), timeout=3):
                print(f"[ready] {host}:{port}")
                return True
        except Exception as exc:
            print(f"[wait {i}/{attempts}] {host}:{port} not ready: {exc}")
            time.sleep(delay)
    return False


kafka_host, kafka_port = KAFKA_BOOTSTRAP_SERVERS.split(":")
if not _wait_for_host(MYSQL_HOST, 3306):
    raise SystemExit(f"[fatal] MySQL '{MYSQL_HOST}' not reachable")
if not _wait_for_host(kafka_host, int(kafka_port)):
    raise SystemExit(f"[fatal] Kafka '{KAFKA_BOOTSTRAP_SERVERS}' not reachable")


def _ensure_topic(bootstrap_servers, topic, partitions=1, replication=1):
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=replication)])
        print(f"[kafka] created topic '{topic}'")
    except TopicAlreadyExistsError:
        print(f"[kafka] topic '{topic}' already exists")
    finally:
        admin.close()


_ensure_topic(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)

spark = (
    SparkSession.builder
    .appName("CDRStreamConsumer")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1g"))
    .config("spark.ui.showConsoleProgress", "false")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed = (
    raw_stream
    .select(F.from_json(F.col("value").cast("string"), CDR_SCHEMA).alias("d"))
    .select("d.*")
    .withColumn("DP_DATE", F.to_timestamp("DP_DATE"))
    .filter(F.col("DP_DATE").isNotNull() & F.col("DP_MSISDN").isNotNull())
)

for col in COUNT_COLS:
    parsed = parsed.withColumn(col, F.greatest(F.coalesce(F.col(col), F.lit(0)), F.lit(0)))
for col in FLOAT_COLS:
    parsed = parsed.withColumn(col, F.greatest(F.coalesce(F.col(col), F.lit(0.0)), F.lit(0.0)))
parsed = parsed.withColumn("DP_DATA_VOLUME", F.abs(F.coalesce(F.col("DP_DATA_VOLUME"), F.lit(0.0))))
parsed = parsed.withColumn(
    "PSEUDO_CHURNED",
    F.when(F.col("PSEUDO_CHURNED") == 1, F.lit(1)).otherwise(F.lit(0)),
)


def write_batch(batch_df, batch_id):
    count = batch_df.count()
    if count == 0:
        return
    (
        batch_df.write
        .format("jdbc")
        .mode("append")
        .option("url", JDBC_URL)
        .option("dbtable", "DP_CDR_Data")
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASS)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("numPartitions", "1")
        .save()
    )
    print(f"[batch {batch_id}] wrote {count} rows to DP_CDR_Data")


query = (
    parsed.writeStream
    .foreachBatch(write_batch)
    .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
    .option("checkpointLocation", CHECKPOINT_DIR)
    .start()
)

print(f"[streaming] listening on topic '{KAFKA_TOPIC}' (trigger={TRIGGER_SECONDS}s, checkpoint={CHECKPOINT_DIR})")
query.awaitTermination()
