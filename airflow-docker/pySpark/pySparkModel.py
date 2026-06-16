import hashlib
import os
import sys
import socket
import time
import json
import tempfile
from datetime import datetime, timedelta

import mlflow
import mysql.connector
from mlflow.tracking import MlflowClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import StorageLevel
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array

from model_card import build_model_card


MYSQL_DB = os.getenv("MYSQL_DATABASE", "RawData")
MYSQL_USER = os.getenv("MYSQL_USER", "spark")
MYSQL_PASS = os.getenv("MYSQL_PASSWORD", "sparkpw")
PREDICTIONS_WRITE_MODE = os.getenv("MODEL_PREDICTIONS_WRITE_MODE", "append")
PIPELINE_RUN_ID = os.getenv("PIPELINE_RUN_ID", f"manual-{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}")
MODEL_VERSION_ID = os.getenv("MODEL_VERSION_ID", f"{PIPELINE_RUN_ID}-random-forest")
MODEL_NAME = os.getenv("MODEL_NAME", "customer_churn_random_forest")
GIT_SHA = os.getenv("GIT_SHA", "unknown")
IMAGE_TAG = os.getenv("IMAGE_TAG", "pyspark-app:latest")
MODEL_ARTIFACT_URI = os.getenv("MODEL_ARTIFACT_URI", f"/tmp/models/{MODEL_VERSION_ID}")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MLFLOW_EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME", "customer_churn")
MLFLOW_REGISTERED_MODEL_NAME = os.getenv("MLFLOW_REGISTERED_MODEL_NAME", MODEL_NAME)
MLFLOW_REGISTER_MODEL = os.getenv("MLFLOW_REGISTER_MODEL", "true").lower() == "true"
MLFLOW_MODEL_ARTIFACT_PATH = os.getenv("MLFLOW_MODEL_ARTIFACT_PATH", "model")
MODEL_TRAINING_MAX_ROWS = int(os.getenv("MODEL_TRAINING_MAX_ROWS", "250000"))
MODEL_CV_FOLDS = int(os.getenv("MODEL_CV_FOLDS", "2"))
MODEL_NUM_TREES = int(os.getenv("MODEL_NUM_TREES", "50"))
MODEL_MAX_DEPTH = [int(d) for d in os.getenv("MODEL_MAX_DEPTH", "5,10").split(",")]
DVC_METRICS_PATH = os.getenv("DVC_METRICS_PATH")
DVC_MODEL_CARD_PATH = os.getenv("DVC_MODEL_CARD_PATH")
DVC_RUN_IDS_PATH = os.getenv("DVC_RUN_IDS_PATH")
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "3g")
SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")
SPARK_SQL_SHUFFLE_PARTITIONS = os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "4")
SPARK_DEFAULT_PARALLELISM = os.getenv("SPARK_DEFAULT_PARALLELISM", SPARK_SQL_SHUFFLE_PARTITIONS)


def wait_for_host(host, port=3306, attempts=20, delay=3):
    for i in range(1, attempts + 1):
        try:
            ip = socket.gethostbyname(host)
            with socket.create_connection((host, port), timeout=3):
                print(f"[mysql ready] {host} ({ip}):{port}")
                return True
        except Exception as e:
            print(f"[mysql wait {i}/{attempts}] {host}:{port} not ready: {e}")
            time.sleep(delay)
    return False


MYSQL_HOST = os.getenv("MYSQL_HOST", "local-mysql")

JDBC_URL = (
    f"jdbc:mysql://{MYSQL_HOST}:3306/{MYSQL_DB}"
    "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
)

print(f"[cfg] host={MYSQL_HOST} db={MYSQL_DB} user={MYSQL_USER}")

if not wait_for_host(MYSQL_HOST, 3306, attempts=20, delay=3):
    raise SystemExit(f"[fatal] MySQL host '{MYSQL_HOST}' not reachable after retries")

START = os.getenv("PROCESSED_START")
END = os.getenv("PROCESSED_END")

JDBC_PROPS = {
    "user": MYSQL_USER,
    "password": MYSQL_PASS,
    "driver": "com.mysql.cj.jdbc.Driver",
    "useCursorFetch": "true",
    "useServerPrepStmts": "true",
    "defaultFetchSize": "1000",
}

TABLE = "Processed_Data"


def mysql_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASS,
        database=MYSQL_DB,
    )


def fetch_scalar(sql, params=None):
    with mysql_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(sql, params or ())
        return cursor.fetchone()[0]


def upsert_pipeline_run(status, prediction_count=None):
    raw_count = fetch_scalar("SELECT COUNT(*) FROM DP_CDR_Data")
    processed_count = fetch_scalar("SELECT COUNT(*) FROM Processed_Data")

    with mysql_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO pipeline_runs (
                run_id, dag_id, git_sha, image_tag, data_start, data_end,
                raw_row_count, processed_row_count, prediction_row_count, status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                git_sha = VALUES(git_sha),
                image_tag = VALUES(image_tag),
                data_start = VALUES(data_start),
                data_end = VALUES(data_end),
                raw_row_count = VALUES(raw_row_count),
                processed_row_count = VALUES(processed_row_count),
                prediction_row_count = VALUES(prediction_row_count),
                status = VALUES(status)
            """,
            (
                PIPELINE_RUN_ID,
                os.getenv("AIRFLOW_DAG_ID", "local_dev_pipeline"),
                GIT_SHA,
                IMAGE_TAG,
                START,
                END,
                raw_count,
                processed_count,
                prediction_count,
                status,
            ),
        )
        conn.commit()


def ensure_model_metadata_schema():
    required_model_version_columns = {
        "mlflow_run_id": "VARCHAR(250)",
        "mlflow_model_uri": "VARCHAR(512)",
        "promotion_status": "VARCHAR(32) NOT NULL DEFAULT 'candidate'",
        "promotion_stage": "VARCHAR(32) NOT NULL DEFAULT 'None'",
        "promotion_reason": "TEXT",
        "promoted_at": "TIMESTAMP NULL",
    }
    required_prediction_columns = {
        "pipeline_run_id": "VARCHAR(250)",
        "model_version_id": "VARCHAR(250)",
        "label": "DOUBLE",
        "prediction": "DOUBLE",
        "probability_0": "DOUBLE",
        "probability_1": "DOUBLE",
        "Date": "DATETIME",
    }

    with mysql_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id VARCHAR(250) PRIMARY KEY,
                dag_id VARCHAR(250),
                git_sha VARCHAR(64),
                image_tag VARCHAR(250),
                data_start DATE,
                data_end DATE,
                raw_row_count BIGINT,
                processed_row_count BIGINT,
                prediction_row_count BIGINT,
                status VARCHAR(32) NOT NULL DEFAULT 'started',
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS model_versions (
                model_version_id VARCHAR(250) PRIMARY KEY,
                run_id VARCHAR(250) NOT NULL,
                model_name VARCHAR(128) NOT NULL,
                algorithm VARCHAR(128) NOT NULL,
                parameters_json TEXT,
                metrics_json TEXT,
                artifact_uri VARCHAR(512),
                mlflow_run_id VARCHAR(250),
                mlflow_model_uri VARCHAR(512),
                promotion_status VARCHAR(32) NOT NULL DEFAULT 'candidate',
                promotion_stage VARCHAR(32) NOT NULL DEFAULT 'None',
                promotion_reason TEXT,
                promoted_at TIMESTAMP NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_model_versions_run (run_id),
                INDEX idx_model_versions_mlflow_run (mlflow_run_id),
                INDEX idx_model_versions_promotion (promotion_status, promotion_stage)
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS model_predictions (
                pipeline_run_id VARCHAR(250),
                model_version_id VARCHAR(250),
                label DOUBLE,
                prediction DOUBLE,
                probability_0 DOUBLE,
                probability_1 DOUBLE,
                Date DATETIME,
                INDEX idx_predictions_run (pipeline_run_id),
                INDEX idx_predictions_model_version (model_version_id),
                INDEX idx_predictions_date (Date),
                INDEX idx_predictions_label (label)
            )
            """
        )
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'model_versions'
            """
        )
        existing_columns = {row[0] for row in cursor.fetchall()}
        for column_name, column_type in required_model_version_columns.items():
            if column_name not in existing_columns:
                cursor.execute(f"ALTER TABLE model_versions ADD COLUMN {column_name} {column_type}")

        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'model_predictions'
            """
        )
        existing_prediction_columns = {row[0] for row in cursor.fetchall()}
        for column_name, column_type in required_prediction_columns.items():
            if column_name not in existing_prediction_columns:
                cursor.execute(f"ALTER TABLE model_predictions ADD COLUMN {column_name} {column_type}")
        conn.commit()


def upsert_model_version(metric, mlflow_run_id=None, mlflow_model_uri=None):
    params = {
        "numTrees": [50],
        "maxDepth": [5, 10],
        "numFolds": max(2, min(MODEL_CV_FOLDS, int(model_min_class_count))),
        "training_max_rows": MODEL_TRAINING_MAX_ROWS,
        "model_input_rows": model_input_count,
        "features": feature_cols,
    }
    metrics = {"auc": metric} if metric is not None else {}

    with mysql_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO model_versions (
                model_version_id, run_id, model_name, algorithm,
                parameters_json, metrics_json, artifact_uri,
                mlflow_run_id, mlflow_model_uri
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                parameters_json = VALUES(parameters_json),
                metrics_json = VALUES(metrics_json),
                artifact_uri = VALUES(artifact_uri),
                mlflow_run_id = VALUES(mlflow_run_id),
                mlflow_model_uri = VALUES(mlflow_model_uri)
            """,
            (
                MODEL_VERSION_ID,
                PIPELINE_RUN_ID,
                MODEL_NAME,
                "RandomForestClassifier",
                json.dumps(params, sort_keys=True),
                json.dumps(metrics, sort_keys=True),
                MODEL_ARTIFACT_URI,
                mlflow_run_id,
                mlflow_model_uri,
            ),
        )
        conn.commit()


def register_mlflow_model(run_id):
    mlflow.log_artifacts(MODEL_ARTIFACT_URI, artifact_path=MLFLOW_MODEL_ARTIFACT_PATH)

    if not MLFLOW_REGISTER_MODEL:
        return

    client = MlflowClient(tracking_uri=MLFLOW_TRACKING_URI, registry_uri=MLFLOW_TRACKING_URI)
    try:
        client.create_registered_model(MLFLOW_REGISTERED_MODEL_NAME)
    except Exception as exc:
        if "RESOURCE_ALREADY_EXISTS" not in str(exc) and "already exists" not in str(exc):
            raise

    source = f"{mlflow_run.info.artifact_uri.rstrip('/')}/{MLFLOW_MODEL_ARTIFACT_PATH}"
    client.create_model_version(
        name=MLFLOW_REGISTERED_MODEL_NAME,
        source=source,
        run_id=run_id,
        tags={
            "pipeline_run_id": PIPELINE_RUN_ID,
            "model_version_id": MODEL_VERSION_ID,
            "artifact_format": "spark_cross_validator_model",
        },
    )

spark = (
    SparkSession.builder
    .config("spark.sql.shuffle.partitions", SPARK_SQL_SHUFFLE_PARTITIONS)
    .config("spark.default.parallelism", SPARK_DEFAULT_PARALLELISM)
    .config("spark.ui.showConsoleProgress", "false")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
    .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
    .config("spark.executor.instances", "1")
    .getOrCreate()
)

print(f"[cfg] host={MYSQL_HOST} db={MYSQL_DB} user={MYSQL_USER}")
if START and END:
    print(f"[cfg] date window: {START} .. {END}")


def load_df_with_query(sql):
    return (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", f"({sql}) t")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("fetchsize", "1000")
        .options(**{k: v for k, v in JDBC_PROPS.items() if k not in ("user", "password", "driver")})
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
label_col = "M_TENURE_CHURN"

if label_col not in cols:
    raise SystemExit(f"[fatal] Column '{label_col}' not found in {TABLE}.")

if "Date" not in cols:
    raise SystemExit(f"[fatal] Column 'Date' not found in {TABLE}; monitoring requires timestamps.")

feature_cols = [c for c in cols if c not in ("Date", "User", label_col)]
select_cols = [c for c in cols if c != "User"]

narrow_select = ", ".join([f"`{c}`" for c in select_cols])
print(f"[features] {feature_cols}")

where_clause = ""
if START and END:
    where_clause = f"WHERE Date >= '{START}' AND Date <= '{END}'"

main_sql = f"SELECT {narrow_select} FROM {TABLE} {where_clause}"

predicates = []
if START and END:
    start_dt = datetime.strptime(START, "%Y-%m-%d")
    end_dt = datetime.strptime(END, "%Y-%m-%d")
    max_days = 14
    days = (end_dt - start_dt).days + 1
    if days > 1 and days <= max_days:
        for i in range(days):
            d0 = (start_dt + timedelta(days=i)).strftime("%Y-%m-%d")
            d1 = (start_dt + timedelta(days=i + 1)).strftime("%Y-%m-%d")
            predicates.append(f"Date >= '{d0}' AND Date < '{d1}'")

if predicates:
    print(f"[jdbc predicates] {len(predicates)} shards")
    df = (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", f"({main_sql}) t")
        .option("fetchsize", "1000")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASS)
        .option("predicates", predicates)
        .load()
    )
else:
    df = load_df_with_query(main_sql)

print("\n=== INPUT SNAPSHOT (final) ===")
non_empty = df.limit(1).count()
print(f"[has any rows] {bool(non_empty)}")
df.show(10, truncate=False)

usable = df.where(F.col(label_col).isNotNull())

has_labeled = usable.limit(1).count()
print(f"[has labeled rows] {bool(has_labeled)}")

label_cnt = usable.select(label_col).distinct().limit(3).count()
print(f"[distinct label count (capped)] {label_cnt}")

if (not has_labeled) or label_cnt < 2:
    raise SystemExit("[fatal] Not enough labeled data to train (need rows from at least 2 classes).")

label_distribution = usable.groupBy(label_col).count().orderBy(label_col)
print("[label distribution]")
label_distribution.show()

label_counts = {row[label_col]: row["count"] for row in label_distribution.collect()}
usable_count = sum(label_counts.values())
min_class_count = min(label_counts.values())
if min_class_count < 2:
    raise SystemExit("[fatal] Each label class needs at least 2 rows for cross-validation.")

if MODEL_TRAINING_MAX_ROWS > 0 and usable_count > MODEL_TRAINING_MAX_ROWS:
    sample_fraction = MODEL_TRAINING_MAX_ROWS / float(usable_count)
    fractions = {label: sample_fraction for label in label_counts}
    model_input = usable.sampleBy(label_col, fractions=fractions, seed=42)
    model_input = model_input.persist(StorageLevel.MEMORY_AND_DISK)
    model_input_count = model_input.count()
    print(
        f"[training cap] sampled {model_input_count} of {usable_count} labeled rows "
        f"(target={MODEL_TRAINING_MAX_ROWS}, fraction={sample_fraction:.6f})"
    )
else:
    model_input = usable.persist(StorageLevel.MEMORY_AND_DISK)
    model_input_count = usable_count
    print(f"[training cap] using all {model_input_count} labeled rows")

model_label_distribution = model_input.groupBy(label_col).count().orderBy(label_col)
print("[model input label distribution]")
model_label_distribution.show()
model_min_class_count = model_label_distribution.agg(F.min("count").alias("min_count")).collect()[0]["min_count"]
if model_min_class_count < 2:
    raise SystemExit("[fatal] Training sample needs at least 2 rows from each class.")

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features",
    handleInvalid="skip"
)

label_indexer = StringIndexer(
    inputCol=label_col,
    outputCol="label",
    handleInvalid="skip"
)

rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    seed=42
)

pipeline = Pipeline(stages=[label_indexer, assembler, rf])

param_grid = (
    ParamGridBuilder()
    .addGrid(rf.numTrees, [MODEL_NUM_TREES])
    .addGrid(rf.maxDepth, MODEL_MAX_DEPTH)
    .build()
)

evaluator = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction"
)

train_df, test_df = model_input.randomSplit([0.8, 0.2], seed=42)
train_df = train_df.persist(StorageLevel.MEMORY_AND_DISK)
test_df = test_df.persist(StorageLevel.MEMORY_AND_DISK)
train_count = train_df.count()
test_count = test_df.count()
print(f"[split] train={train_count}  test={test_count}")

if train_count == 0:
    raise SystemExit("[fatal] Train split ended up empty.")

train_label_count = train_df.select(label_col).distinct().count()
if train_label_count < 2:
    print("[warn] Train split has one class; using bounded model input for training.")
    train_df = model_input
    train_count = train_df.count()
    test_df = model_input
    test_count = test_df.count()

cv = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=param_grid,
    evaluator=evaluator,
    numFolds=max(2, min(MODEL_CV_FOLDS, int(model_min_class_count))),
    parallelism=1
)

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)
mlflow_run = mlflow.start_run(run_name=MODEL_VERSION_ID)
mlflow_run_id = mlflow_run.info.run_id
mlflow_model_uri = f"runs:/{mlflow_run_id}/{MLFLOW_MODEL_ARTIFACT_PATH}"

mlflow.set_tags(
    {
        "pipeline_run_id": PIPELINE_RUN_ID,
        "model_version_id": MODEL_VERSION_ID,
        "model_name": MODEL_NAME,
        "registered_model_name": MLFLOW_REGISTERED_MODEL_NAME,
        "algorithm": "RandomForestClassifier",
        "git_sha": GIT_SHA,
        "image_tag": IMAGE_TAG,
        "mysql_database": MYSQL_DB,
        "data_start": START or "",
        "data_end": END or "",
    }
)
mlflow.log_params(
    training_params := {
        "numTrees": MODEL_NUM_TREES,
        "maxDepth": ",".join(str(d) for d in MODEL_MAX_DEPTH),
        "numFolds": max(2, min(MODEL_CV_FOLDS, int(model_min_class_count))),
        "training_max_rows": MODEL_TRAINING_MAX_ROWS,
        "model_input_rows": model_input_count,
        "feature_count": len(feature_cols),
        "label_col": label_col,
        "predictions_write_mode": PREDICTIONS_WRITE_MODE,
    }
)
with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as feature_file:
    json.dump({"features": feature_cols}, feature_file, sort_keys=True)
    feature_file_path = feature_file.name
mlflow.log_artifact(feature_file_path, artifact_path="metadata")
os.unlink(feature_file_path)

cv_model = cv.fit(train_df)
print("[model] trained OK")
cv_model.write().overwrite().save(MODEL_ARTIFACT_URI)
print(f"[model] saved artifact to {MODEL_ARTIFACT_URI}")
register_mlflow_model(mlflow_run_id)
print(f"[mlflow] run_id={mlflow_run_id} model_uri={mlflow_model_uri}")

# Log Gini feature importances from the fitted RandomForest stage
try:
    rf_stage = cv_model.bestModel.stages[-1]
    importances = rf_stage.featureImportances.toArray()
    importance_dict = {col: float(imp) for col, imp in zip(feature_cols, importances.tolist())}
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as fi_file:
        json.dump(importance_dict, fi_file, indent=2)
        fi_path = fi_file.name
    mlflow.log_artifact(fi_path, artifact_path="explanation")
    os.unlink(fi_path)

    # Save a sample of training data as SHAP background dataset
    background = train_df.select(feature_cols).limit(100).toPandas()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as bg_file:
        background.to_csv(bg_file, index=False)
        bg_path = bg_file.name
    mlflow.log_artifact(bg_path, artifact_path="explanation")
    os.unlink(bg_path)
    print(f"[mlflow] logged feature importances and SHAP background ({len(background)} rows)")
except Exception as _exc:
    print(f"[mlflow] WARNING: could not log explanation artifacts: {_exc}")

feature_hash = hashlib.sha256(json.dumps(sorted(feature_cols)).encode()).hexdigest()[:12]
print(f"[features] hash={feature_hash}  cols={feature_cols}")
mlflow.log_param("feature_hash", feature_hash)

metric = None
if test_count > 0:
    predictions = cv_model.transform(test_df)
    metric = evaluator.evaluate(predictions)
    print(f"[auc] {metric:.4f}")
    mlflow.log_metric("auc", float(metric))
else:
    print("[note] test set empty; using train set predictions for persistence.")
    predictions = cv_model.transform(train_df)

prediction_output = (
    predictions
    .withColumn("probability_array", vector_to_array("probability"))
    .withColumn("probability_0", F.col("probability_array").getItem(0))
    .withColumn("probability_1", F.col("probability_array").getItem(1))
    .withColumn(
        "Date",
        F.coalesce(F.to_timestamp("Date"), F.current_timestamp())
    )
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID))
    .withColumn("model_version_id", F.lit(MODEL_VERSION_ID))
    .select(
        F.col("pipeline_run_id"),
        F.col("model_version_id"),
        F.col("label").cast("double").alias("label"),
        F.col("prediction").cast("double").alias("prediction"),
        F.col("probability_0").cast("double").alias("probability_0"),
        F.col("probability_1").cast("double").alias("probability_1"),
        F.col("Date")
    )
)

prediction_count = prediction_output.count()
print(f"[prediction_output.count] {prediction_count}")
prediction_output.show(10, truncate=False)

precision_at_top_decile = None
try:
    from pyspark.sql import Window as _Window
    _top_n = max(1, prediction_count // 10)
    _win = _Window.orderBy(F.desc("probability_1"))
    _top = (
        prediction_output
        .withColumn("_rank", F.row_number().over(_win))
        .filter(F.col("_rank") <= _top_n)
    )
    _top_total = _top.count()
    _top_churned = _top.filter(F.col("label") == 1.0).count()
    precision_at_top_decile = _top_churned / _top_total if _top_total > 0 else 0.0
    mlflow.log_metric("precision_at_top_decile", float(precision_at_top_decile))
    print(f"[precision@top10] {precision_at_top_decile:.4f}  ({_top_churned}/{_top_total} churned in top decile)")
except Exception as _exc:
    print(f"[precision@top10] WARNING: could not compute: {_exc}")

mlflow.log_metrics(
    row_metrics := {
        "train_rows": train_count,
        "test_rows": test_count,
        "prediction_rows": prediction_count,
    }
)

if prediction_count == 0:
    raise SystemExit("[fatal] No prediction rows produced; model_predictions was not written.")

ensure_model_metadata_schema()
upsert_pipeline_run("model_trained", prediction_count=prediction_count)
upsert_model_version(metric, mlflow_run_id=mlflow_run_id, mlflow_model_uri=mlflow_model_uri)

(
    prediction_output.write
    .format("jdbc")
    .mode(PREDICTIONS_WRITE_MODE)
    .option("url", JDBC_URL)
    .option("dbtable", f"{MYSQL_DB}.model_predictions")
    .option("user", MYSQL_USER)
    .option("password", MYSQL_PASS)
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("numPartitions", "1")
    .option("sessionInitStatement", "SET @spark_tag='model_predictions_write'")
    .save()
)

print(f"[write] model_predictions {PREDICTIONS_WRITE_MODE} done")
upsert_pipeline_run("predictions_written", prediction_count=prediction_count)

model_card_metrics = {
    "auc": float(metric) if metric is not None else None,
    "precision_at_top_decile": float(precision_at_top_decile) if precision_at_top_decile is not None else None,
}
model_card = build_model_card(
    pipeline_run_id=PIPELINE_RUN_ID,
    model_version_id=MODEL_VERSION_ID,
    model_name=MODEL_NAME,
    registered_model_name=MLFLOW_REGISTERED_MODEL_NAME,
    algorithm="RandomForestClassifier",
    git_sha=GIT_SHA,
    image_tag=IMAGE_TAG,
    mlflow_run_id=mlflow_run_id,
    mlflow_model_uri=mlflow_model_uri,
    artifact_uri=MODEL_ARTIFACT_URI,
    data_start=START,
    data_end=END,
    features=feature_cols,
    feature_hash=feature_hash,
    metrics=model_card_metrics,
    params=training_params,
    row_counts={
        "model_input_rows": model_input_count,
        "train_rows": train_count,
        "test_rows": test_count,
        "prediction_rows": prediction_count,
    },
    promotion_policy={
        "min_model_auc": os.getenv("MIN_MODEL_AUC"),
        "min_precision_at_top_decile": os.getenv("MIN_PRECISION_AT_TOP_DECILE"),
        "min_promotion_prediction_rows": os.getenv("MIN_PROMOTION_PREDICTION_ROWS"),
    },
)

with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as model_card_file:
    json.dump(model_card, model_card_file, indent=2, sort_keys=True)
    model_card_tmp_path = model_card_file.name
mlflow.log_artifact(model_card_tmp_path, artifact_path="metadata")

if DVC_MODEL_CARD_PATH:
    os.makedirs(os.path.dirname(os.path.abspath(DVC_MODEL_CARD_PATH)), exist_ok=True)
    with open(DVC_MODEL_CARD_PATH, "w") as _f:
        json.dump(model_card, _f, indent=2, sort_keys=True)
    print(f"[dvc] model card written to {DVC_MODEL_CARD_PATH}")
os.unlink(model_card_tmp_path)

mlflow.end_run(status="FINISHED")

if DVC_METRICS_PATH:
    dvc_metrics = {
        "pipeline_run_id": PIPELINE_RUN_ID,
        "model_version_id": MODEL_VERSION_ID,
        "auc": float(metric) if metric is not None else None,
        "precision_at_top_decile": float(precision_at_top_decile) if precision_at_top_decile is not None else None,
        "train_rows": train_count,
        "test_rows": test_count,
        "prediction_rows": prediction_count,
        "feature_count": len(feature_cols),
        "feature_hash": feature_hash,
    }
    os.makedirs(os.path.dirname(os.path.abspath(DVC_METRICS_PATH)), exist_ok=True)
    with open(DVC_METRICS_PATH, "w") as _f:
        json.dump(dvc_metrics, _f, indent=2)
    print(f"[dvc] metrics written to {DVC_METRICS_PATH}")

if DVC_RUN_IDS_PATH:
    os.makedirs(os.path.dirname(os.path.abspath(DVC_RUN_IDS_PATH)), exist_ok=True)
    with open(DVC_RUN_IDS_PATH, "w") as _f:
        _f.write(f"PIPELINE_RUN_ID={PIPELINE_RUN_ID}\n")
        _f.write(f"MODEL_VERSION_ID={MODEL_VERSION_ID}\n")
    print(f"[dvc] run IDs written to {DVC_RUN_IDS_PATH}")

print("[done] pySparkModel completed successfully.")
