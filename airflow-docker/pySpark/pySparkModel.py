import os
import json
import time
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit, coalesce, when, count
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# ───────────── helpers ─────────────
def read_config(path="/app/config.json"):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def get_cfg():
    cfg = read_config()
    db = cfg.get("database", {})
    tbl = cfg.get("tables", {})
    dates = cfg.get("dates", {})

    host = os.getenv("MYSQL_HOST", db.get("host", "local-mysql"))
    port = int(os.getenv("MYSQL_PORT", db.get("port", 3306)))
    user = os.getenv("MYSQL_USER", db.get("user", "root"))
    pwd  = os.getenv("MYSQL_PASSWORD", db.get("password", ""))
    dbn  = os.getenv("MYSQL_DATABASE", db.get("db", "RawData"))

    start = os.getenv("PROCESSED_START", dates.get("start", "2024-10-01"))
    end   = os.getenv("PROCESSED_END", dates.get("end", "2025-12-31"))

    processed_table = tbl.get("processed", "Processed_Data")

    return {
        "host": host, "port": port, "user": user, "pwd": pwd, "db": dbn,
        "processed_table": processed_table,
        "start": start, "end": end
    }

def build_jdbc_url(host, port, db):
    return f"jdbc:mysql://{host}:{port}/{db}?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"

def jdbc_props(user, pwd):
    return {
        "user": user,
        "password": pwd,
        "driver": "com.mysql.cj.jdbc.Driver"
    }

def check_data_availability(spark, jdbc_url, props, cfg):
    """Check if processed data is available"""

    print("\n=== Checking Data Availability ===")

    try:
        # Get total count
        total_query = f"(SELECT COUNT(*) as total_count FROM {cfg['db']}.{cfg['processed_table']}) t"
        total_df = spark.read.jdbc(url=jdbc_url, table=total_query, properties=props)
        total_count = total_df.collect()[0]['total_count']
        print(f"Total rows in {cfg['processed_table']}: {total_count}")

        if total_count == 0:
            return None, "Table is empty"

        # Get date range
        date_query = f"""(SELECT 
            MIN(Date) as min_date, 
            MAX(Date) as max_date,
            COUNT(DISTINCT Date) as unique_dates
            FROM {cfg['db']}.{cfg['processed_table']}) t"""
        date_df = spark.read.jdbc(url=jdbc_url, table=date_query, properties=props)
        date_info = date_df.collect()[0]

        print(f"Date range: {date_info['min_date']} to {date_info['max_date']}")
        print(f"Unique dates: {date_info['unique_dates']}")

        # Check label distribution
        label_query = f"""(SELECT 
            M_TENURE_CHURN, 
            COUNT(*) as cnt 
            FROM {cfg['db']}.{cfg['processed_table']} 
            GROUP BY M_TENURE_CHURN) t"""
        label_df = spark.read.jdbc(url=jdbc_url, table=label_query, properties=props)
        print("\nLabel distribution:")
        label_df.show()

        return total_count, "OK"

    except Exception as e:
        return None, str(e)

def load_and_prepare_data(spark, jdbc_url, props, cfg):
    """Load and prepare data for modeling"""

    print("\n=== Loading Data ===")

    # Try different loading strategies
    strategies = [
        ("Date filtered query",
         f"""(SELECT * FROM {cfg['db']}.{cfg['processed_table']} 
              WHERE Date >= '{cfg['start']}' 
              AND Date <= '{cfg['end']}') t"""),

        ("All data",
         f"(SELECT * FROM {cfg['db']}.{cfg['processed_table']}) t"),

        ("Recent 30 days",
         f"""(SELECT * FROM {cfg['db']}.{cfg['processed_table']} 
              WHERE Date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)) t"""),

        ("Sample 100k rows",
         f"(SELECT * FROM {cfg['db']}.{cfg['processed_table']} LIMIT 100000) t")
    ]

    df = None
    for strategy_name, query in strategies:
        try:
            print(f"\nTrying: {strategy_name}")
            test_df = spark.read.jdbc(url=jdbc_url, table=query, properties=props)
            count = test_df.count()
            print(f"Result: {count} rows")

            if count > 0:
                df = test_df
                print(f"✅ Using {strategy_name}")
                break
        except Exception as e:
            print(f"Failed: {e}")
            continue

    if df is None:
        raise RuntimeError("Could not load any data!")

    return df

def prepare_features(df):
    """Prepare features for modeling"""

    print("\n=== Preparing Features ===")

    # Define feature columns
    feature_cols = [
        "M_Out_Call_Count",
        "M_Out_Call_Time",
        "M_Data_Sum",
        "M_Data_Count",
        "M_In_Call_Count",
        "M_In_Call_Time",
    ]

    label_col = "M_TENURE_CHURN"

    # Ensure all columns exist and handle nulls
    for col_name in feature_cols:
        if col_name not in df.columns:
            print(f"⚠️ Missing column {col_name}, adding with zeros")
            df = df.withColumn(col_name, lit(0.0))
        else:
            # Replace nulls with 0
            df = df.withColumn(col_name, coalesce(col(col_name).cast("double"), lit(0.0)))

    # Ensure label column exists
    if label_col not in df.columns:
        print(f"⚠️ Missing label column {label_col}, adding with zeros")
        df = df.withColumn(label_col, lit(0))
    else:
        df = df.withColumn(label_col, coalesce(col(label_col).cast("int"), lit(0)))

    # Add engineered features
    df = df.withColumn("total_calls",
                       col("M_Out_Call_Count") + col("M_In_Call_Count"))

    df = df.withColumn("total_call_time",
                       col("M_Out_Call_Time") + col("M_In_Call_Time"))

    df = df.withColumn("avg_call_duration",
                       when(col("total_calls") > 0,
                            col("total_call_time") / col("total_calls"))
                       .otherwise(0.0))

    df = df.withColumn("data_per_session",
                       when(col("M_Data_Count") > 0,
                            col("M_Data_Sum") / col("M_Data_Count"))
                       .otherwise(0.0))

    # Update feature columns with engineered features
    feature_cols.extend(["total_calls", "total_call_time", "avg_call_duration", "data_per_session"])

    # Remove rows where all features are zero
    condition = lit(False)
    for col_name in feature_cols:
        condition = condition | (col(col_name) > 0)

    df = df.filter(condition)

    # Check for data issues
    print("\nFeature statistics:")
    df.select(feature_cols + [label_col]).describe().show()

    # Check label distribution
    print("\nLabel distribution after preparation:")
    df.groupBy(label_col).count().show()

    # Check for nulls
    print("\nNull counts:")
    null_counts = []
    for col_name in feature_cols + [label_col]:
        null_count = df.filter(col(col_name).isNull()).count()
        null_counts.append((col_name, null_count))

    for col_name, null_count in null_counts:
        if null_count > 0:
            print(f"  {col_name}: {null_count} nulls")

    return df, feature_cols, label_col

def build_and_train_model(train_df, test_df, feature_cols, label_col):
    """Build and train the model"""

    print("\n=== Building Model Pipeline ===")

    # Create pipeline stages
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features_raw",
        handleInvalid="skip"
    )

    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withStd=True,
        withMean=False
    )

    rf = RandomForestClassifier(
        labelCol=label_col,
        featuresCol="features",
        numTrees=50,
        maxDepth=10,
        seed=42,
        subsamplingRate=0.8,
        featureSubsetStrategy="sqrt"
    )

    # Create pipeline
    pipeline = Pipeline(stages=[assembler, scaler, rf])

    # Create parameter grid for tuning
    paramGrid = ParamGridBuilder() \
        .addGrid(rf.numTrees, [20, 50]) \
        .addGrid(rf.maxDepth, [5, 10]) \
        .build()

    # Create evaluator
    evaluator = BinaryClassificationEvaluator(
        labelCol=label_col,
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )

    # Create cross validator
    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=paramGrid,
        evaluator=evaluator,
        numFolds=3,
        seed=42
    )

    print(f"Training with {train_df.count()} samples...")

    try:
        # Train model
        cv_model = cv.fit(train_df)

        # Get best model
        best_model = cv_model.bestModel

        print("✅ Model training successful!")

        # Make predictions on test set
        predictions = cv_model.transform(test_df)

        # Evaluate model
        auc = evaluator.evaluate(predictions)
        print(f"\n📊 Model Performance:")
        print(f"  AUC-ROC: {auc:.4f}")

        # Additional metrics
        accuracy_evaluator = MulticlassClassificationEvaluator(
            labelCol=label_col,
            predictionCol="prediction",
            metricName="accuracy"
        )

        f1_evaluator = MulticlassClassificationEvaluator(
            labelCol=label_col,
            predictionCol="prediction",
            metricName="f1"
        )

        accuracy = accuracy_evaluator.evaluate(predictions)
        f1 = f1_evaluator.evaluate(predictions)

        print(f"  Accuracy: {accuracy:.4f}")
        print(f"  F1 Score: {f1:.4f}")

        # Show confusion matrix
        print("\nConfusion Matrix:")
        predictions.groupBy(label_col, "prediction").count().show()

        # Show sample predictions
        print("\nSample Predictions:")
        predictions.select(label_col, "prediction", "probability").show(10, truncate=False)

        # Feature importance (from the RandomForest model)
        rf_model = best_model.stages[-1]
        feature_importance = rf_model.featureImportances

        print("\nFeature Importance:")
        for i, col in enumerate(feature_cols):
            if i < len(feature_importance):
                print(f"  {col}: {feature_importance[i]:.4f}")

        return cv_model

    except Exception as e:
        print(f"❌ Model training failed: {e}")
        raise

    # ───────────── main ─────────────
if __name__ == "__main__":
    print("\n" + "="*80)
    print("PYSPARK MODEL - CHURN PREDICTION")
    print("="*80)

    cfg = get_cfg()

    print(f"\nConfiguration:")
    print(f"  Host: {cfg['host']}")
    print(f"  Database: {cfg['db']}")
    print(f"  Table: {cfg['processed_table']}")
    print(f"  Date Range: {cfg['start']} to {cfg['end']}")

    spark = (
        SparkSession.builder
        .appName("pySparkModel")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    jdbc_url = build_jdbc_url(cfg["host"], cfg["port"], cfg["db"])
    props = jdbc_props(cfg["user"], cfg["pwd"])

    # Check data availability
    count, status = check_data_availability(spark, jdbc_url, props, cfg)

    if count is None or count == 0:
        print(f"\n❌ ERROR: No data available in {cfg['processed_table']}")
        print(f"Status: {status}")
        print("\nPossible solutions:")
        print("1. Run PySparkAnalysis.py first to process raw data")
        print("2. Check if the raw data table has data")
        print("3. Verify database connection settings")

        # Try to check raw data
        try:
            raw_query = f"(SELECT COUNT(*) as cnt FROM {cfg['db']}.DP_CDR_Data) t"
            raw_df = spark.read.jdbc(url=jdbc_url, table=raw_query, properties=props)
            raw_count = raw_df.collect()[0]['cnt']
            print(f"\nRaw data table (DP_CDR_Data) has {raw_count} rows")
        except Exception as e:
            print(f"Could not check raw table: {e}")

        spark.stop()
        exit(1)

    # Load and prepare data
    try:
        df = load_and_prepare_data(spark, jdbc_url, props, cfg)

        # Prepare features
        df, feature_cols, label_col = prepare_features(df)

        # Check if we have enough data
        total_count = df.count()
        print(f"\nTotal samples after preparation: {total_count}")

        if total_count < 100:
            print("⚠️ WARNING: Very few samples for training. Results may be unreliable.")

        # Check label balance
        label_counts = df.groupBy(label_col).count().collect()
        for row in label_counts:
            label_val = row[label_col]
            count = row["count"]
            pct = (count / total_count) * 100
            print(f"  Class {label_val}: {count} samples ({pct:.1f}%)")

        # Split data
        print("\n=== Splitting Data ===")
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

        train_count = train_df.count()
        test_count = test_df.count()

        print(f"Training set: {train_count} samples")
        print(f"Test set: {test_count} samples")

        if train_count == 0:
            print("❌ ERROR: Empty training set!")
            spark.stop()
            exit(1)

        if test_count == 0:
            print("⚠️ WARNING: Empty test set, using training set for evaluation")
            test_df = train_df

        # Build and train model
        model = build_and_train_model(train_df, test_df, feature_cols, label_col)

        # Save model
        try:
            model_path = "/app/models/churn_model"
            print(f"\n=== Saving Model to {model_path} ===")
            model.write().overwrite().save(model_path)
            print("✅ Model saved successfully")
        except Exception as e:
            print(f"⚠️ Could not save model: {e}")

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        print("\nDebug information:")
        print("1. Check if PySparkAnalysis.py ran successfully")
        print("2. Verify that Processed_Data table has data")
        print("3. Check MySQL connection settings")

        import traceback
        traceback.print_exc()

        spark.stop()
        exit(1)

    print("\n" + "="*80)
    print("✅ PYSPARK MODEL COMPLETE!")
    print("="*80)

    spark.stop()