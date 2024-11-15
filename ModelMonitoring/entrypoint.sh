#!/bin/bash

# Run the Flask app in the background
python /app/Model_Monitoring.py &

# Run the database polling in the foreground
python /app/polling_db.py
