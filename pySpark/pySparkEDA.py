import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
import os
import time
from pyspark.sql import Row, DataFrame, functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from time import gmtime, strftime, time
import datetime
import sys
from pyspark.sql.functions import skewness
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
spark = SparkSession.builder \
        .appName("EDA") \
        .getOrCreate()

df_20241010 = spark.read.parquet('parquetFiles/processed_data_2024-10-10_130521.parquet')
df_20241011 = spark.read.parquet('parquetFiles/processed_data_2024-10-11_122412.parquet')
df_20241016 = spark.read.parquet('parquetFiles/processed_data_2024-10-16_124447.parquet')

df = df_20241010.union(df_20241011).union(df_20241016)

df_check = df.filter(df["M_TENURE_CHURN"] == 0)
count_df = df_check.groupBy("M_TENURE_CHURN").count()
results = count_df.collect()

# for row in results:
#     print(f"Value: {row["M_TENURE_CHURN"]}, Count: {row["count"]}')

def summary_all_columns(df):
    group_col = 'M_TENURE_CHURN'
    columns = [col for col in df.columns if col != group_col]
    agg_exprs = []
    for col in columns:
        agg_exprs.append(F.sum(col).alias(f'{col}_sum'))
        agg_exprs.append(F.avg(col).alias(f'{col}_avg'))
        agg_exprs.append(F.min(col).alias(f'{col}_min'))
        agg_exprs.append(F.max(col).alias(f'{col}_max'))
        agg_exprs.append(F.count(col).alias(f'{col}_count'))
        agg_exprs.append(F.variance(col).alias(f'{col}_variance'))
        agg_exprs.append(F.skewness(col).alias(f'{col}_skewness'))
    summary_df = df.groupBy(group_col).agg(*agg_exprs)
    return summary_df
summary_columns = summary_all_columns(df)
summary_columns.show(truncate=False)

def summary_statistics(df):

    # Columns in dataset
    print("Columns:", df.columns)
    # Count of all records
    print("Record Count:", df.count())

    columns = df.columns
    null_count = df.select([F.count(F.when(F.col(column).isNull(), column)).alias(column) for column in columns])
    # null values
    print("Null Count:", null_count.count())
    # Distinct Subscriber
    print("Distinct Subscriber: ", df.select("User").distinct().count())
    # Churned User Counts
    churn_count = df.groupBy("M_TENURE_CHURN").count()
    churn_results = churn_count.collect()
    for row in churn_results:
        print(f'Value: {row["M_TENURE_CHURN"]}, Count: {row["count"]}')
    # Unprocessed Skewness for M_DATA_SUM
    print("Skewness Measure w/ no Transformation:", df.agg({'M_DATA_SUM': 'skewness'}).collect()[0][0])
    # Unprocessed Mean for M_DATA_SUM
    print("Mean Measuer w/ no Transformation:", df.agg({'M_DATA_SUM': 'mean'}).collect()[0][0])
    # Standard Deviaton for Unprocessed M_DATA_SUM
    print("Standard Deviation w/ no Transformation:", df.agg({'M_DATA_SUM': 'stddev'}).collect()[0][0])

sumStats = summary_statistics(df)

# sns.heatmap(data=null_count, cmap="YlGn")
# plt.xticks(rotation=30, fontsize=10)
# plt.yticks(rotation=0, fontsize=10)
# plt.show()

def correlation_calculation(df):
    columns_excluding_last = df.columns[:-1]
    numeric_cols = [field.name for field in df.schema.fields if field.name in columns_excluding_last
                    and isinstance(field.dataType, (IntegerType, DoubleType, FloatType))]


    # Define variables to store the maximum correlation and its corresponding column
    corr_max = float('-inf')  # Start with the lowest possible value
    corr_max_col = None
    for col in numeric_cols:
        corr_val = df.corr('M_TENURE_CHURN', col)
        if corr_val > corr_max:
            corr_max = corr_val
            corr_max_col = col
    # Print the column with the highest correlation
    print(f"Column with the highest correlation to 'M_TENURE_CHURN': {corr_max_col}, Correlation value: {corr_max}")
    pandas_df = df.select(corr_max_col).dropna().toPandas()
    return  pandas_df, corr_max_col
pandas_df_corr, corr_max_col = correlation_calculation(df)

pandas_df = pandas_df_corr[pandas_df_corr[corr_max_col] > 0]
pandas_df['log_transformed'] = np.log1p(pandas_df[corr_max_col])
print("log transformed:", pandas_df_corr)

print(pandas_df['log_transformed'].mean())
print(pandas_df['log_transformed'].std())

def column_dropper(df, threshold):
    total_records = df.count()
    for col in df.columns:
        missing = df.where(df[col].isNull()).count()
        missing_percent = missing / total_records
        if missing_percent > threshold:
            df = df.drop(col, axis=1)
    return df

df = column_dropper(df, 0.6)
# Calculate correlation with 'M_TENURE_CHURN' for each numeric column



def min_max_scaler_with_transformations(df, cols_to_scale):
    # Step 1: Apply min-max scaling
    for col in cols_to_scale:
        max_val = df.agg({col: 'max'}).collect()[0][0]
        min_val = df.agg({col: 'min'}).collect()[0][0]
        new_column_name = 'scaled_' + col

        df = df.withColumn(new_column_name, (df[col] - min_val) / (max_val - min_val))

    # Step 2: Calculate skewness, mean, and standard deviation for one of the columns
    skewness = df.agg({'scaled_M_DATA_SUM': 'skewness'}).collect()[0][0]
    mean = df.agg({'scaled_M_DATA_SUM': 'mean'}).collect()[0][0]
    stddev = df.agg({'scaled_M_DATA_SUM': 'stddev'}).collect()[0][0]

    # Step 3: Perform reflection and transformation for 'scaled_M_DATA_SUM'
    max_data_sum = df.agg({'scaled_M_DATA_SUM': 'max'}).collect()[0][0]
    df = df.withColumn('Reflect_DATA_SUM', (max_data_sum + 1) - df['scaled_M_DATA_SUM'])
    df = df.withColumn('adj_DATA_SUM', 1 / F.log(df['Reflect_DATA_SUM']))

    # Step 4: Select the relevant columns to show
    result_df = df.select('scaled_M_DATA_SUM', 'Reflect_DATA_SUM', 'adj_DATA_SUM')

    # Step 5: Return the transformed DataFrame and statistics
    return result_df, skewness, mean, stddev

# Call the function
scaled_df, skewness, mean, stddev = min_max_scaler_with_transformations(df, cols_to_scale=['M_TENURE_CHURN', 'M_DATA_SUM', 'M_DATA_COUNT'])

# Show the scaled data
scaled_df.show()

# Print the calculated statistics
print(f"Skewness: {skewness}")
print(f"Mean: {mean}")
print(f"Standard Deviation: {stddev}")

# Convert the scaled data to Pandas DataFrame for plotting
pandas_df = scaled_df.toPandas()

# Step 6: Plot the results using matplotlib
def plot_transformed_data(pandas_df, column):
    # Check if the log_transformed column exists
    if column not in pandas_df.columns:
        print(f"Column '{column}' not found in DataFrame!")
        return

    # Plotting with matplotlib instead of seaborn to avoid further seaborn issues
    plt.figure(figsize=(8, 6))
    plt.hist(pandas_df[column], bins=30, alpha=0.7, color='blue', label=column)
    plt.title(f'Distribution of {column}')
    plt.xlabel(column)
    plt.ylabel('Frequency')
    plt.legend()

    # Save the plot to a file instead of sending it to a local server
    plt.savefig('log_transformed_distribution.png')  # Save to a file
    plt.show()  # Show the plot interactively


# Example usage assuming the transformation has been applied correctly
# and pandas_df is created properly
pandas_df['log_transformed'] = np.log1p(pandas_df['scaled_M_DATA_SUM'])  # Example transformation

# Plot the data
plot_transformed_data(pandas_df, 'log_transformed')
# print("null values:", scaled.where(scaled['M_DATA_SUM'].isNull().count()))
# pandas_2 = scaled.dropna().toPandas()

# plt.figure(figsize= (10, 6))
# sns.histplot(data=pandas_2, x='adj_DATA_SUM', kde=True)
# plt.show()






plt.figure(figsize=(10, 6))
sns.histplot(data=pandas_df, x='log_transformed', kde=True)
plt.title(f"Distribution of {corr_max_col}")
plt.xlabel(corr_max_col)
plt.show()

"""
Creating New Features
"""
