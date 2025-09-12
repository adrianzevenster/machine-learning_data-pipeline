#!/usr/bin/env python3
import os, sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta
import socket, time


MYSQL_DB   = os.getenv("MYSQL_DATABASE", "RawData")
MYSQL_USER = os.getenv("MYSQL_USER", "spark")
MYSQL_PASS = os.getenv("MYSQL_PASSWORD", "sparkpw")

def wait_for_host(host, port=3306, attempts=20, delay=3):
    for i in range(1, attempts + 1):
        try:
            # DNS first (will raise on unknown host)
            ip = socket.gethostbyname(host)
            # then TCP connect
            with socket.create_connection((host, port), timeout=3):
                print(f"[mysql ready] {host} ({ip}):{port}")
                return True
        except Exception as e:
            print(f"[mysql wait {i}/{attempts}] {host}:{port} not ready: {e}")
            time.sleep(delay)
    return False

# normalize host if something odd slips in
MYSQL_HOST = os.getenv("MYSQL_HOST", "local-mysql")  # keep your original default
if MYSQL_HOST in {"mysql", "db", "flaskapp-flaskapp-db-1"}:
    print(f"[note] Overriding MYSQL_HOST '{MYSQL_HOST}' -> 'local-mysql'")
    MYSQL_HOST = "local-mysql"

# (re)build JDBC_URL AFTER normalization
JDBC_URL = (
    f"jdbc:mysql://{MYSQL_HOST}:3306/{MYSQL_DB}"
    "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
)


print(f"[cfg] host={MYSQL_HOST} db={MYSQL_DB} user={MYSQL_USER}")

# wait for DNS + port before Spark touches JDBC
if not wait_for_host(MYSQL_HOST, 3306, attempts=20, delay=3):
    raise SystemExit(f"[fatal] MySQL host '{MYSQL_HOST}' not reachable after retries")

START = os.getenv("PROCESSED_START")   # e.g. '2025-08-22'
END   = os.getenv("PROCESSED_END")     # e.g. '2025-08-22'


JDBC_PROPS = {
    "user": MYSQL_USER,
    "password": MYSQL_PASS,
    "driver": "com.mysql.cj.jdbc.Driver",
    "useCursorFetch": "true",
    "useServerPrepStmts": "true",
    "defaultFetchSize": "1000",
}

TABLE = "Processed_Data"

spark = (
    SparkSession.builder
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.default.parallelism", "4")
    .config("spark.ui.showConsoleProgress", "false")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.driver.memory", "3g")
    .config("spark.executor.memory", "2g")
    .config("spark.executor.instances", "1")
    .getOrCreate()
)

print(f"[cfg] host={MYSQL_HOST} db={MYSQL_DB} user={MYSQL_USER}")
if START and END:
    print(f"[cfg] date window: {START} .. {END}")

base_select = "*"
label_col   = "M_TENURE_CHURN"

def load_df_with_query(sql):
    return (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", f"({sql}) t")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("fetchsize", "1000")       # ← Spark will call setFetchSize(1000)
        .options(**{k: v for k, v in JDBC_PROPS.items() if k not in ("user","password","driver")})
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASS)
        .load()
    )


schema_peek_where = ""
if START and END:
    schema_peek_where = f"WHERE Date >= '{START}' AND Date <= '{END}'"
schema_peek_sql = f"SELECT * FROM {TABLE} {schema_peek_where} LIMIT 50"
peek = load_df_with_query(schema_peek_sql)
print("\n=== INPUT SNAPSHOT (peek) ===")
print(f"[peek rows] {peek.count()}")
peek.printSchema()
peek.show(10, truncate=False)

cols = peek.columns
if label_col not in cols:
    print(f"[warn] Column '{label_col}' not found -> nothing to train. Exiting successfully.")
    sys.exit(0)

feature_cols = [c for c in cols if c not in ("Date", "User", label_col)]
narrow_select = ", ".join([*(f"`{c}`" for c in feature_cols), f"`{label_col}`"])
print(f"[features] {feature_cols}")


where_clause = ""
if START and END:
    where_clause = f"WHERE Date >= '{START}' AND Date <= '{END}'"
main_sql = f"SELECT {narrow_select} FROM {TABLE} {where_clause}"

predicates = []
if START and END:
    start_dt = datetime.strptime(START, "%Y-%m-%d")
    end_dt   = datetime.strptime(END, "%Y-%m-%d")
    # cap shard count to avoid thousands of small tasks
    max_days = 14
    days = (end_dt - start_dt).days + 1
    if days > 1 and days <= max_days:
        for i in range(days):
            d0 = (start_dt + timedelta(days=i)).strftime("%Y-%m-%d")
            d1 = (start_dt + timedelta(days=i+1)).strftime("%Y-%m-%d")
            predicates.append(f"Date >= '{d0}' AND Date < '{d1}'")

if predicates:
    print(f"[jdbc predicates] {len(predicates)} shards")
    df = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", f"({main_sql}) t")
        .option("fetchsize", "1000")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("user", MYSQL_USER).option("password", MYSQL_PASS)
        .option("predicates", predicates)   # ← split by day
        .load()
    )
else:
    df = load_df_with_query(main_sql)

# AFTER (bounded actions)
print("\n=== INPUT SNAPSHOT (final) ===")
non_empty = df.limit(1).count()  # at most 1 row pull
print(f"[has any rows] {bool(non_empty)}")
df.show(10, truncate=False)

usable = df.where(F.col(label_col).isNotNull())

# Only check “has labels” without scanning the table
has_labeled = usable.limit(1).count()
print(f"[has labeled rows] {bool(has_labeled)}")

# Distinct label check but cap to 3 unique values
label_cnt = usable.select(label_col).distinct().limit(3).count()
print(f"[distinct label count (capped)] {label_cnt}")

if (not has_labeled) or label_cnt < 2:
    print("[warn] Not enough labeled data (need >=1 row and >=2 classes). Exiting successfully.")
    sys.exit(0)

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator

assembler     = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")
label_indexer = StringIndexer(inputCol=label_col, outputCol="label", handleInvalid="skip")
rf            = RandomForestClassifier(featuresCol="features", labelCol="label", seed=42)

pipeline = Pipeline(stages=[label_indexer, assembler, rf])

param_grid = (ParamGridBuilder()
              .addGrid(rf.numTrees, [50])     # ← trim grid to keep memory down
              .addGrid(rf.maxDepth, [5, 10])
              .build())

evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction")

train_df, test_df = usable.randomSplit([0.8, 0.2], seed=42)
print(f"[split] train={train_df.count()}  test={test_df.count()}")

if train_df.count() == 0:
    print("[warn] train split ended up empty -> exiting successfully.")
    sys.exit(0)

cv = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=param_grid,
    evaluator=evaluator,
    numFolds=3,
    parallelism=1
)

cv_model = cv.fit(train_df)
print("[model] trained OK")

if test_df.count() > 0:
    metric = evaluator.evaluate(cv_model.transform(test_df))
    print(f"[auc] {metric:.4f}")
else:
    print("[note] test set empty; skipping evaluation.")

print("[done] pySparkModel completed successfully.")
