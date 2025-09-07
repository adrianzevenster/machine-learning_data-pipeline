#!/usr/bin/env python3
import os, sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# -----------------------
# Config from environment
# -----------------------
MYSQL_HOST = os.getenv("MYSQL_HOST", "local-mysql")
MYSQL_DB   = os.getenv("MYSQL_DATABASE", "RawData")
MYSQL_USER = os.getenv("MYSQL_USER", "spark")
MYSQL_PASS = os.getenv("MYSQL_PASSWORD", "sparkpw")

START = os.getenv("PROCESSED_START")   # e.g. '2025-08-22'
END   = os.getenv("PROCESSED_END")     # e.g. '2025-08-22'

JDBC_URL = (
    f"jdbc:mysql://{MYSQL_HOST}:3306/{MYSQL_DB}"
    "?sslMode=REQUIRED&enabledTLSProtocols=TLSv1.2,TLSv1.3"
    "&allowPublicKeyRetrieval=true&serverTimezone=UTC"
)

JDBC_PROPS = {
    "user": MYSQL_USER,
    "password": MYSQL_PASS,
    "driver": "com.mysql.cj.jdbc.Driver",
}

TABLE = "Processed_Data"   # this is what your features looked like in the logs

# -----------------------
# Spark session
# -----------------------
spark = (
    SparkSession.builder
    .getOrCreate()
)

print(f"[cfg] host={MYSQL_HOST} db={MYSQL_DB} user={MYSQL_USER}")
if START and END:
    print(f"[cfg] date window: {START} .. {END}")

# -----------------------
# Load data
# -----------------------
if START and END:
    # filter by Date column (case-sensitive as your table showed)
    query = f"(SELECT * FROM {TABLE} WHERE Date >= '{START}' AND Date <= '{END}') t"
else:
    query = TABLE

try:
    df = spark.read.jdbc(url=JDBC_URL, table=query, properties=JDBC_PROPS)
except Exception as e:
    print("[fatal] Could not read Processed_Data:", e)
    # Fail loudly here: if we cannot even read, let Airflow fail the task.
    raise

print("\n=== INPUT SNAPSHOT ===")
print(f"[rows total] {df.count()}")
df.printSchema()
df.show(10, truncate=False)

# -----------------------
# Basic validity checks
# -----------------------
if "M_TENURE_CHURN" not in df.columns:
    print("[warn] Column 'M_TENURE_CHURN' not found -> nothing to train. Exiting successfully.")
    sys.exit(0)

usable = df.where(F.col("M_TENURE_CHURN").isNotNull())
n_total = usable.count()
print(f"[rows with label] {n_total}")

labels = [r[0] for r in usable.select("M_TENURE_CHURN").distinct().collect()]
print(f"[distinct labels] {labels}")

if n_total == 0 or len(labels) < 2:
    print("[warn] Not enough labeled data (need >=1 row and >=2 classes). Exiting successfully.")
    sys.exit(0)

# -----------------------
# Proceed with ML only if viable
# -----------------------
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator

feature_cols = [c for c in df.columns
                if c not in ("Date", "User", "M_TENURE_CHURN")]

print(f"[features] {feature_cols}")

label_indexer = StringIndexer(inputCol="M_TENURE_CHURN", outputCol="label", handleInvalid="skip")
assembler     = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")
rf            = RandomForestClassifier(featuresCol="features", labelCol="label", seed=42)

pipeline = Pipeline(stages=[label_indexer, assembler, rf])

param_grid = (ParamGridBuilder()
              .addGrid(rf.numTrees, [50, 100])
              .addGrid(rf.maxDepth, [5, 10])
              .build())

evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction")

# train / test split with guard
train_df, test_df = usable.randomSplit([0.8, 0.2], seed=42)
print(f"[split] train={train_df.count()}  test={test_df.count()}")

if train_df.count() == 0:
    print("[warn] train split ended up empty -> exiting successfully.")
    sys.exit(0)

cv = CrossValidator(estimator=pipeline,
                    estimatorParamMaps=param_grid,
                    evaluator=evaluator,
                    numFolds=3,
                    parallelism=1)  # keep resource usage tame

cv_model = cv.fit(train_df)
print("[model] trained OK")

# Evaluate (if we have test rows)
if test_df.count() > 0:
    metric = evaluator.evaluate(cv_model.transform(test_df))
    print(f"[auc] {metric:.4f}")
else:
    print("[note] test set empty; skipping evaluation.")

print("[done] pySparkModel completed successfully.")
