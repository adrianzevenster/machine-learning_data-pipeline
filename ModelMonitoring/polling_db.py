import time
import requests
import mysql.connector

def check_for_updates(last_check):
    connection = mysql.connector.connect(
        host='flaskapp-flaskapp-db-1',
        user='root',
        password='a?xBVq1!',
        database='RawData'
    )
    cursor = connection.cursor()
    cursor.execute("SELECT MAX(Date) FROM model_predictions")
    latest_date = cursor.fetchone()[0]
    cursor.close()
    connection.close()

    return (latest_date > last_check if last_check else True), latest_date

def trigger_monitoring():
    response = requests.get('http://localhost:5001/run-monitoring')
    if response.status_code == 200:
        print("Monitoring triggered successfully.")
    else:
        print(f"Failed to trigger monitoring: {response.status_code}, {response.text}")

last_check = None

while True:
    try:
        has_update, last_check = check_for_updates(last_check)
        if has_update:
            print("New data found, triggering monitoring...")
            trigger_monitoring()
        else:
            print("No new data, waiting...")
        time.sleep(60)  # Poll every 60 seconds
    except Exception as e:
        print(f"Error during polling: {e}")
        time.sleep(60)
