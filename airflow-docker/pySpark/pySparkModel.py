# pySparkModel.py (defensive)
import os, json, sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.functions import vector_to_array

MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_DB   = os.getenv("MYSQL_DATABASE", "RawData")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PWD  = os.getenv("MYSQL_PASSWORD", "")

spark = (SparkSession.builder
         .appName("cv_model")
         .config("spark.jars","/opt/spark/jars/mysql-connector-java-8.0.25.jar")
         .getOrCreate())

# Load model window
cfg_path = os.getenv("CONFIG_PATH", "/app/config.json")
with open(cfg_path) as f:
    cfg = json.load(f)
p_start, p_end = cfg["processed_start"], cfg["processed_end"]

db_url  = f"jdbc:mysql://{MYSQL_HOST}:3306/{MYSQL_DB}?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"
props   = {"user": MYSQL_USER, "password": MYSQL_PWD, "driver": "com.mysql.cj.jdbc.Driver"}

# Robust date cast (works even if Date is stored as text)
query = f"(SELECT * FROM {MYSQL_DB}.Processed_Data " \
        f" WHERE STR_TO_DATE(Date,'%Y-%m-%d') BETWEEN '{p_start}' AND '{p_end}') t"

df = spark.read.jdbc(url=db_url, table=query, properties=props)
cnt = df.count()
print(f"[Model] Loaded Processed_Data rows in [{p_start}..{p_end}]: {cnt}")
df.printSchema()
df.show(10, truncate=False)
if cnt == 0:
    print("[Model] ❌ Empty dataset for the selected window. Widen processed_start/processed_end in config.json.", file=sys.stderr)
    sys.exit(2)

# Label column can be named two ways – detect it
label_col = next((c for c in ["M_Tenure_Churn","M_TENURE_CHURN"] if c in df.columns), None)
if not label_col:
    print("[Model] ❌ Label column not found. Columns:", df.columns, file=sys.stderr)
    sys.exit(3)
df = df.withColumn("label", col(label_col).cast("int"))

# Feature engineering
base = ['M_Out_Call_Count','M_Out_Call_Time','M_Data_Sum','M_Data_Count']
for c in base:
    df = df.withColumn(c+"_sum",   F.sum(c).over(Window.partitionBy('Date')))
    df = df.withColumn(c+"_mean",  F.mean(c).over(Window.partitionBy('Date')))
    df = df.withColumn(c+"_stddev",F.stddev(c).over(Window.partitionBy('Date')))

feats = base + [f"{c}_{s}" for c in base for s in ["sum","mean","stddev"]]
for c in feats:
    df = df.withColumn(c+"_log", F.log1p(F.col(c)))
feats = feats + [c+"_log" for c in feats]

df = df.fillna(0)
df = VectorAssembler(inputCols=feats, outputCol="features").transform(df)

# Class balance sanity check
c0 = df.filter(col("label")==0).count()
c1 = df.filter(col("label")==1).count()
print(f"[Model] class_0={c0}, class_1={c1}")
if c0 == 0 or c1 == 0:
    print("[Model] ❌ Need both classes present in the selected window. Pick a wider/different date range.", file=sys.stderr)
    sys.exit(4)

# Train
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
rf = RandomForestClassifier(featuresCol="features", labelCol="label")
paramGrid = (ParamGridBuilder()
             .addGrid(rf.numTrees, [50, 100])
             .addGrid(rf.maxDepth, [5, 10])
             .build())
evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="prediction", metricName="areaUnderROC")
cv = CrossValidator(estimator=rf, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=3)

cv_model = cv.fit(train_df)  # safe now
pred = cv_model.transform(test_df)
pred = (pred
        .withColumn("probability_array", vector_to_array("probability"))
        .withColumn("rawPrediction_array", vector_to_array("rawPrediction"))
        .withColumn("features_array", vector_to_array("features"))
        .withColumn("probability_0", F.col("probability_array")[0])
        .withColumn("probability_1", F.col("probability_array")[1])
        .withColumn("rawPrediction_0", F.col("rawPrediction_array")[0])
        .withColumn("rawPrediction_1", F.col("rawPrediction_array")[1])
        .drop("probability","probability_array","rawPrediction","rawPrediction_array","features","features_array"))

roc_auc = evaluator.evaluate(pred)
print(f"[Model] ROC-AUC: {roc_auc}")
pred.write.jdbc(url=db_url, table="model_predictions", mode="append", properties=props)
print("[Model] ✅ Predictions written to model_predictions")
