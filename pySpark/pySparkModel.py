from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.functions import vector_to_array
import json

'''Spark Session Creator: cv_model'''
spark = SparkSession.builder \
    .appName("cv_model") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.memory.offHeap.enable", True) \
    .config("spark.memory.offHeap.size", "4g") \
    .config("spark.jars", "/opt/spark/jars/mysql-connector-java-8.0.25.jar") \
    .getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", 200)
# "/home/adrian/.config/JetBrains/PyCharm2024.1/jdbc-drivers/MySQL ConnectorJ/8.2.0/com/mysql/mysql-connector-j/8.2.0/mysql-connector-j-8.2.0.jar")
'''Database Connection for model results'''
# start_date = "2024-10-15"
# end_date = "2024-11-04"
with open('/app/config.json', 'r') as config_file:
    config = json.load(config_file)

start_date = config['start_date']
end_date = config['end_date']
# Use a SQL query to filter dates within the specified range
query = f"(SELECT * FROM Processed_Data WHERE Date BETWEEN '{start_date}' AND '{end_date}') AS date_filtered_data"

# Connect to the database and read filtered data
db_url = "jdbc:mysql://flaskapp-db:3306/RawData"
db_table = "model_predictions"
db_properties = {
    "user": "root",
    "password": "a?xBVq1!",
    "driver": "com.mysql.cj.jdbc.Driver"
}


'''Reading and Joining Parquet files'''
df = spark.read.jdbc(
    url=db_url,
    table=query,
    properties=db_properties
)

df.show()

# Step 1: Calculate sum, mean, and standard deviation for specific columns
columns_to_aggregate = ['M_Out_Call_Count', 'M_Out_Call_Time', 'M_Data_Sum', 'M_Data_Count']

for col in columns_to_aggregate:
    # Add sum feature
    sum_col_name = col + '_sum'
    df = df.withColumn(sum_col_name, F.sum(col).over(Window.partitionBy('Date')))

    # Add mean feature
    mean_col_name = col + '_mean'
    df = df.withColumn(mean_col_name, F.mean(col).over(Window.partitionBy('Date')))

    # Add standard deviation feature
    stddev_col_name = col + '_stddev'
    df = df.withColumn(stddev_col_name, F.stddev(col).over(Window.partitionBy('Date')))

# Step 2: Include both original and new features in the feature vector
feature_columns_initial = [
    # Original columns
    'M_Out_Call_Count', 'M_Out_Call_Time', 'M_Data_Sum', 'M_Data_Count',

    # Sum, mean, and standard deviation features
    'M_Out_Call_Count_sum', 'M_Out_Call_Count_mean', 'M_Out_Call_Count_stddev',
    'M_Out_Call_Time_sum', 'M_Out_Call_Time_mean', 'M_Out_Call_Time_stddev',
    'M_Data_Sum_sum', 'M_Data_Sum_mean', 'M_Data_Sum_stddev',
    'M_Data_Count_sum', 'M_Data_Count_mean', 'M_Data_Count_stddev'
]

# Generate log features
# Generate log features (renaming the loop variable to avoid conflict)
for column_name in feature_columns_initial:
    log_col_name = column_name + '_log'
    df = df.withColumn(log_col_name, F.log1p(F.col(column_name)))

log_feature_columns = [col + '_log' for col in feature_columns_initial]
feature_columns = feature_columns_initial + log_feature_columns

# Step 3: Fill or handle null/NaN values
df = df.fillna(0)  # Fill NaN values with 0 (or use the column mean)

# Step 4: Assemble the feature vector
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
df_transformed = assembler.transform(df)

# Step 5: Rename the target column (M_TENURE_CHURN) to "label"
df_final = df_transformed.withColumnRenamed("M_TENURE_CHURN", "label")

# Step 6: Oversample the minority class (label == 1)
df_class_0 = df_final.filter(df_final.label == 0)
df_class_1 = df_final.filter(df_final.label == 1)

oversampled_class_1 = df_class_1.sample(withReplacement=True, fraction=20.0, seed=42)
balanced_df = df_class_0.union(oversampled_class_1)

# Step 7: Split the balanced data into train and test sets
train_df, test_df = balanced_df.randomSplit([0.8, 0.2], seed=42)

# Step 8: Initialize the Random Forest Classifier
rf = RandomForestClassifier(featuresCol='features', labelCol='label', predictionCol="prediction",
                            probabilityCol="probability")

# Step 9: Define a parameter grid for hyperparameter tuning
paramGrid = ParamGridBuilder() \
    .addGrid(rf.numTrees, [50, 100]) \
    .addGrid(rf.maxDepth, [5, 10]) \
    .build()
# .addGrid(rf.featureSubsetStrategy, ['auto', 'sqrt', 'log2']) \
# .addGrid(rf.impurity, ['gini', 'entropy']) \


# Step 10: Define the evaluator, using "probability" column for evaluation
evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="prediction", metricName="areaUnderROC")

# Step 11: Set up cross-validation
cv = CrossValidator(estimator=rf, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=3)

# Step 12: Train the model using cross-validation
cv_model = cv.fit(train_df)

# Step 13: Make predictions
predictions = cv_model.transform(test_df)
# predictions.printSchema()
# Convert 'probability' vector to an array and then extract elements
predictions_to_save = predictions \
    .withColumn("probability_array", vector_to_array("probability")) \
    .withColumn("rawPrediction_array", vector_to_array("rawPrediction")) \
    .withColumn("features_array", vector_to_array("features"))

# Extract individual elements from probability and rawPrediction arrays (assuming binary classification)
predictions_to_save = predictions_to_save \
    .withColumn("probability_0", F.col("probability_array")[0]) \
    .withColumn("probability_1", F.col("probability_array")[1]) \
    .withColumn("rawPrediction_0", F.col("rawPrediction_array")[0]) \
    .withColumn("rawPrediction_1", F.col("rawPrediction_array")[1])

# Optionally, if you want to convert the array of features into a string or JSON format for storage:
predictions_to_save = predictions_to_save.withColumn("features_str", F.concat_ws(",", F.col("features_array")))

# Drop the original vector columns
predictions_to_save = predictions_to_save.drop("probability", "probability_array", "rawPrediction", "rawPrediction_array", "features", "features_array")

# Step 14: Evaluate the model
roc_auc = evaluator.evaluate(predictions)
print(f"ROC-AUC Score with Random Forest: {roc_auc}")

# predictions_to_save = predictions_to_save.drop("probability", "probability_array")

predictions_to_save.printSchema()
predictions_to_save.write.jdbc(url=db_url, table=db_table, mode='append', properties=db_properties)
print(f"Data written to {db_table} successfully.")
# columns_to_aggregate = ['M_Out_Call_Count', 'M_Out_Call_Time', 'M_Data_Sum', 'M_Data_Count']
#
# '''Feature Creation'''
# for col in columns_to_aggregate:
#     sum_col_name = col + '_sum'
#     df = df.withColumn(sum_col_name, F.sum(col).over(Window.partitionBy()))
#
#     mean_col_name = col + '_mean'
#     df = df.withColumn(mean_col_name, F.mean(col).over(Window.partitionBy()))
#
#     stddev_col_name = col + '_stddev'
#     df = df.withColumn(stddev_col_name, F.stddev(col).over(Window.partitionBy()))
#
#     log_col_name = col + '_log'
#     df = df.withColumn(log_col_name, F.log(col).over(Window.partitionBy()))
#
# df.fillna(0)
#
# '''Processing of Features'''
#
# df.show()
# # assembler = VectorAssembler(inputCols=df, outputCol=)