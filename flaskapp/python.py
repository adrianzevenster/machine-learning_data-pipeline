from flask import Flask, jsonify, render_template, send_file
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import io
import base64

app = Flask(__name__)


def fetch_data():
    df = execute_sql_query(
        query="SELECT * FROM RawData.DP_CDR_Data LIMIT 10000",
        database_name="RawData"
    )
    df['DP_DATE'] = pd.to_datetime(df['DP_DATE'])
    return df


def create_random_batches_with_randomized_timestamps(df, batch_size, num_batches):
    for i in range(num_batches):
        batch_df = df.sample(n=batch_size).reset_index(drop=True)
        current_time = datetime.now()
        random_timestamps = [current_time + timedelta(seconds=random.randint(-300, 300)) for _ in range(batch_size)]
        batch_df['DP_DATE'] = random_timestamps
        for col in batch_df.select_dtypes(include=[np.number]).columns:
            noise = np.random.normal(0, 0.1, batch_size)
            batch_df[col] = batch_df[col] * (1 + noise)
        insert_into_database(batch_df)
        print(f"Batch {i + 1} inserted into database.")
        generate_and_save_chart(batch_df, i + 1)


def insert_into_database(df):
    engine = create_engine('mysql+pymysql://root:a?xBVq1!@localhost:3306/RawData')
    df.to_sql('DP_CDR_Data', engine, index=False, if_exists='append')


def generate_and_save_chart(df, batch_number):
    plt.figure(figsize=(10, 6))
    plt.plot(df['DP_DATE'], df['some_numeric_column'])  # Replace with actual numeric column
    plt.title(f'Batch {batch_number} Data Overview')
    plt.xlabel('Time')
    plt.ylabel('Value')

    img = io.BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)
    img_data = base64.b64encode(img.read()).decode('utf-8')
    plt.close()

    engine = create_engine('mysql+pymysql://root:a?xBVq1!@localhost:3306/RawData')
    connection = engine.connect()
    connection.execute(f"INSERT INTO chart_storage (batch_number, chart_image) VALUES ({batch_number}, '{img_data}')")
    connection.close()


@app.route('/data', methods=['GET'])
def get_data():
    engine = create_engine('mysql+pymysql://root:a?xBVq1!@localhost:3306/RawData')
    query = "SELECT * FROM DP_CDR_Data ORDER BY DP_DATE DESC LIMIT 100"
    df = pd.read_sql(query, engine)
    return df.to_json(orient='records')


@app.route('/chart/<int:batch_number>', methods=['GET'])
def get_chart(batch_number):
    engine = create_engine('mysql+pymysql://root:a?xBVq1!@localhost:3306/RawData')
    query = f"SELECT chart_image FROM chart_storage WHERE batch_number = {batch_number}"
    result = engine.execute(query).fetchone()

    if result:
        img_data = base64.b64decode(result[0])
        return send_file(io.BytesIO(img_data), mimetype='image/png')
    else:
        return jsonify({"error": "Chart not found"}), 404


@app.route('/')
def index():
    return 'Welcome to Flask API'


if __name__ == "__main__":

    app.run(debug=True, host='0.0.0.0', port=5001)
