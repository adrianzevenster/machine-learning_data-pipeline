#!/bin/bash

if [ -f GCP-Key.json ]; then
    echo "GCP-Key.json already exists. Skipping creation."
    exit 0
fi

echo "Enter your GCP service account key (JSON content), then press Enter:"
read -r GCP_STORAGE_KEY

echo "$GCP_STORAGE_KEY" > GCP-Key.json
echo "GCP-Key.json created successfully."
