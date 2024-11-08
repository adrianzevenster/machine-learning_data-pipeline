from flask import Flask, jsonify
import nannyml as nml
import pandas as pd
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

app = Flask(__name__)

@app.route('/run-monitoring', methods=['GET'])
def run_monitoring():
    try:
        # Spark session
        spark = SparkSession.builder \
            .appName("Model Monitoring") \
            .config("spark.driver.extraClassPath", "/app/mysql-connector-java-8.0.25.jar") \
            .config("spark.driver.memory", "2g") \
            .getOrCreate()


        # Connect to the database using flaskapp-db as the host
        predictions_df = spark.read.jdbc(
            url="jdbc:mysql://flaskapp-flaskapp-db-1:3306/RawData",
            table="model_predictions",
            properties={"user": "root", "password": "a?xBVq1!"}
        )

        # Convert to pandas DataFrame
        predictions_pd = predictions_df.select("label", "prediction", "probability_0", "probability_1", "Date").toPandas()

        # Initialize NannyML's PerformanceCalculator
        nml_monitor = nml.PerformanceCalculator(
            problem_type='classification',
            y_true='label',
            y_pred='prediction',
            y_pred_proba='probability_1',
            timestamp_column_name='Date',
            metrics=['roc_auc', 'f1', 'accuracy']
        )

        # Fit the reference data
        nml_monitor.fit(reference_data=predictions_pd)

        # Calculate performance metrics
        results = nml_monitor.calculate(predictions_pd)
        # plot = results.plot()
        # plt.draw()

        # fig, ax = plt.subplots()
        # results.plot(ax=ax)
        # fig.canvas.draw()

        results.plot().show()
        plt.draw()
        plt.tight_layout()


        # plot.savefig('app/output/performance_plot.png')
        results.plot().write_image('/app/output/performance_plot.png')

        # Convert the results to a pandas DataFrame and save it to CSV
        results_df = results.to_df()
        results_df.to_csv('/app/output/performance_report.csv', index=False)

        # Return success message
        return jsonify({"message": "Monitoring completed and saved."}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)