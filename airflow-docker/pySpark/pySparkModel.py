import os
import json
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql import SparkSession
from pyspark.ml.functions import vector_to_array

# DB params from ENV
MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DATABASE", "RawData")

spark = (SparkSession.builder
         .appName("cv_model")
         .config("spark.executor.memory", "8g")
         .config("spark.driver.memory", "8g")
         .config("spark.jars", "/opt/spark/jars/mysql-connector-java-8.0.25.jar")
         .getOrCreate())
spark.conf.set("spark.sql.shuffle.partitions", 200)

with open('/app/config.json', 'r') as config_file:
    config = json.load(config_file)
start_date = config['processed_start']
end_date = config['processed_end']

db_url = f"jdbc:mysql://{MYSQL_HOST}:3306/{MYSQL_DB}"
db_properties = {"user": MYSQL_USER, "password": MYSQL_PASSWORD, "driver": "com.mysql.cj.jdbc.Driver"}

query = f"(SELECT * FROM {MYSQL_DB}.Processed_Data WHERE Date BETWEEN '{start_date}' AND '{end_date}') AS date_filtered_data"
df = spark.read.jdbc(url=db_url, table=query, properties=db_properties)
df = df.repartition(200, col('Date')).fillna(0)

# Aggregates
columns_to_aggregate = ['M_Out_Call_Count', 'M_Out_Call_Time', 'M_Data_Sum', 'M_Data_Count']
for c in columns_to_aggregate:
    df = df.withColumn(f"{c}_sum",   F.sum(c).over(Window.partitionBy('Date')))
    df = df.withColumn(f"{c}_mean",  F.mean(c).over(Window.partitionBy('Date')))
    df = df.withColumn(f"{c}_stddev",F.stddev(c).over(Window.partitionBy('Date')))

# Log features
feature_columns_initial = [
    'M_Out_Call_Count','M_Out_Call_Time','M_Data_Sum','M_Data_Count',
    'M_Out_Call_Count_sum','M_Out_Call_Count_mean','M_Out_Call_Count_stddev',
    'M_Out_Call_Time_sum','M_Out_Call_Time_mean','M_Out_Call_Time_stddev',
    'M_Data_Sum_sum','M_Data_Sum_mean','M_Data_Sum_stddev',
    'M_Data_Count_sum','M_Data_Count_mean','M_Data_Count_stddev'
]
for name in feature_columns_initial:
    df = df.withColumn(name + '_log', F.log1p(F.col(name)))

feature_columns = feature_columns_initial + [c + '_log' for c in feature_columns_initial]

assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
df_transformed = assembler.transform(df)

df_final = df_transformed.withColumnRenamed("M_Tenure_Churn", "label")
df0, df1 = df_final.filter("label = 0"), df_final.filter("label = 1")
balanced_df = df0.union(df1.sample(withReplacement=True, fraction=20.0, seed=42))
train_df, test_df = balanced_df.randomSplit([0.8, 0.2], seed=42)

rf = RandomForestClassifier(featuresCol='features', labelCol='label', predictionCol="prediction", probabilityCol="probability")
paramGrid = (ParamGridBuilder()
             .addGrid(rf.numTrees, [50, 100])
             .addGrid(rf.maxDepth, [5, 10])
             .build())
evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="prediction", metricName="areaUnderROC")
cv = CrossValidator(estimator=rf, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=3)

cv_model = cv.fit(train_df)
predictions = cv_model.transform(test_df)

# Flatten vector columns for JDBC sink
predictions_to_save = (predictions
                       .withColumn("probability_array", vector_to_array("probability"))
                       .withColumn("rawPrediction_array", vector_to_array("rawPrediction"))
                       .withColumn("features_array", vector_to_array("features"))
                       .withColumn("probability_0", F.col("probability_array")[0])
                       .withColumn("probability_1", F.col("probability_array")[1])
                       .withColumn("rawPrediction_0", F.col("rawPrediction_array")[0])
                       .withColumn("rawPrediction_1", F.col("rawPrediction_array")[1])
                       .withColumn("features_str", F.concat_ws(",", F.col("features_array")))
                       .drop("probability","probability_array","rawPrediction","rawPrediction_array","features","features_array")
                       )

roc_auc = evaluator.evaluate(predictions)
print(f"ROC-AUC Score with Random Forest: {roc_auc}")

predictions_to_save.write.jdbc(url=db_url, table=f"{MYSQL_DB}.model_predictions", mode='append', properties=db_properties)
print(f"Data written to {MYSQL_DB}.model_predictions successfully.")
