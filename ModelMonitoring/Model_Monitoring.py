from flask import Flask, jsonify
import nannyml as nml
import pandas as pd
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import os

# Initialize Flask app
app = Flask(__name__)

@app.route('/run-monitoring', methods=['GET'])
def run_monitoring():
    try:
        # Ensure output directory exists
        output_dir = '/app/output/'
        os.makedirs(output_dir, exist_ok=True)

        # Spark session
        spark = SparkSession.builder \
            .appName("Model Monitoring") \
            .config("spark.driver.extraClassPath", "/app/mysql-connector-java-8.0.25.jar") \
            .config("spark.driver.memory", "4g") \
            .getOrCreate()

        # Fetch data
        predictions_df = spark.read.jdbc(
            url="jdbc:mysql://flaskapp-flaskapp-db-1:3306/RawData",
            table="model_predictions",
            properties={"user": "root", "password": "a?xBVq1!"}
        )

        predictions_pd = predictions_df.select("label", "prediction", "probability_0", "probability_1", "Date").toPandas()

        # Debug unique labels
        unique_labels = predictions_pd['label'].unique()
        print(f"Unique labels in the dataset: {unique_labels}")

        # Skip processing if single-class data
        if len(unique_labels) < 2:
            warning_message = "Single-class data detected, skipping calculations."
            print(warning_message)
            return jsonify({"warning": warning_message}), 200

        # Initialize NannyML
        nml_monitor = nml.PerformanceCalculator(
            problem_type='classification',
            y_true='label',
            y_pred='prediction',
            y_pred_proba='probability_1',
            timestamp_column_name='Date',
            metrics=['roc_auc', 'f1', 'accuracy']
        )

        # Fit reference data
        nml_monitor.fit(reference_data=predictions_pd)

        # Calculate performance metrics
        results = nml_monitor.calculate(predictions_pd)

        # Save results
        results.plot().write_image(os.path.join(output_dir, 'performance_plot.png'))
        results.to_df().to_csv(os.path.join(output_dir, 'performance_report.csv'), index=False)

        return jsonify({"message": "Monitoring completed and saved."}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500





if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
