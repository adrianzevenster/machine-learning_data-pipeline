from flask import Flask, request, jsonify
import pandas as pd
import numpy as np
import os
import random
import json
from datetime import datetime, timedelta
from main import execute_sql_query
import time
import threading
import logging
from kafka import KafkaProducer
from request_validation import parse_positive_int

app = Flask(__name__)
MAX_BATCH_SIZE = int(os.getenv("MAX_STREAM_BATCH_SIZE", "10000"))
MAX_NUM_BATCHES = int(os.getenv("MAX_STREAM_NUM_BATCHES", "1000"))
MAX_INTERVAL_SECONDS = int(os.getenv("MAX_STREAM_INTERVAL_SECONDS", "3600"))

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_CDR_TOPIC", "cdr-events")

COUNT_COLUMNS = ["DP_MOC_COUNT", "DP_MTC_COUNT", "DP_MOSMS_COUNT", "DP_MTSMS_COUNT", "DP_DATA_COUNT"]
FLOAT_COLUMNS = ["DP_MOC_DURATION", "DP_MTC_DURATION"]
NOISY_METRIC_COLUMNS = COUNT_COLUMNS + FLOAT_COLUMNS + ["DP_DATA_VOLUME"]


def _make_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )


def _serialize_record(record):
    out = {}
    for k, v in record.items():
        if isinstance(v, datetime):
            out[k] = v.isoformat()
        elif hasattr(v, "item"):
            out[k] = v.item()
        else:
            out[k] = v
    return out


def _publish_batch(df, batch_size, batch_num, num_batches, producer):
    logging.debug(f"Creating batch {batch_num}/{num_batches} with size {batch_size}")
    batch_df = df.sample(n=batch_size).reset_index(drop=True)

    current_time = datetime.now()
    batch_df["DP_DATE"] = [
        current_time + timedelta(seconds=random.randint(-300, 300))
        for _ in range(batch_size)
    ]

    for col in NOISY_METRIC_COLUMNS:
        if col in batch_df.columns:
            noise = np.random.normal(0, 0.1, batch_size)
            batch_df[col] = batch_df[col] * (1 + noise)

    if "id" in batch_df.columns:
        batch_df = batch_df.drop(columns=["id"])

    for col in COUNT_COLUMNS:
        if col in batch_df.columns:
            batch_df[col] = pd.to_numeric(batch_df[col], errors="coerce").fillna(0).clip(lower=0).round().astype(int)
    for col in FLOAT_COLUMNS:
        if col in batch_df.columns:
            batch_df[col] = pd.to_numeric(batch_df[col], errors="coerce").fillna(0.0).clip(lower=0)
    if "DP_DATA_VOLUME" in batch_df.columns:
        batch_df["DP_DATA_VOLUME"] = pd.to_numeric(batch_df["DP_DATA_VOLUME"], errors="coerce").fillna(0.0).abs()
    if "PSEUDO_CHURNED" in batch_df.columns:
        batch_df["PSEUDO_CHURNED"] = (
            pd.to_numeric(batch_df["PSEUDO_CHURNED"], errors="coerce")
            .fillna(0)
            .round()
            .clip(lower=0, upper=1)
            .astype(int)
        )

    batch_df = batch_df.where(pd.notnull(batch_df), None)

    for record in batch_df.to_dict(orient="records"):
        producer.send(KAFKA_TOPIC, _serialize_record(record))

    producer.flush()
    logging.info(
        f"Batch {batch_num}/{num_batches} ({batch_size} rows) published to Kafka topic '{KAFKA_TOPIC}'"
    )


def stream_data(df, batch_size=1000, num_batches=10, interval=60):
    logging.info(f"Starting streaming: {num_batches} batches, interval={interval}s")
    producer = _make_producer()
    try:
        for batch_num in range(1, num_batches + 1):
            _publish_batch(df, batch_size, batch_num, num_batches, producer)
            if batch_num < num_batches:
                time.sleep(interval)
    finally:
        producer.close()
    logging.info("Streaming completed.")


logging.basicConfig(level=logging.DEBUG)

stream_thread = None


@app.route("/start_stream", methods=["POST"])
def start_stream():
    global stream_thread
    try:
        payload = request.get_json(silent=True) or {}
        if stream_thread and stream_thread.is_alive():
            return jsonify({"message": "A streaming process is already running"}), 409

        logging.debug("Received request to start streaming.")
        df = execute_sql_query(query="SELECT * FROM DP_CDR_Data LIMIT 10000", database_name="RawData")
        logging.debug(f"DataFrame fetched: {df.head()}")

        batch_size = parse_positive_int(payload, "batch_size", 1000, MAX_BATCH_SIZE)
        num_batches = parse_positive_int(payload, "num_batches", 10, MAX_NUM_BATCHES)
        interval = parse_positive_int(payload, "interval", 60, MAX_INTERVAL_SECONDS)

        logging.debug(f"Streaming config: batch_size={batch_size}, num_batches={num_batches}, interval={interval}")
        stream_thread = threading.Thread(
            target=stream_data, args=(df, batch_size, num_batches, interval)
        )
        stream_thread.start()
        logging.debug("Background thread started.")
        return jsonify({"message": "Streaming started"}), 200
    except ValueError as e:
        logging.warning(f"Invalid stream request: {str(e)}")
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(
        debug=os.getenv("FLASK_DEBUG", "false").lower() == "true",
        host="0.0.0.0",
        port=5000,
        threaded=True,
    )
