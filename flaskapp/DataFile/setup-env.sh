#!/bin/bash

# Check if GCP-Key.json already exists
if [ -f GCP-Key.json ]; then
    echo "GCP-Key.json already exists. Skipping creation."
    exit 0
fi

# Prompt the user to input their GCP key JSON
echo "Enter your GCP service account key (JSON content), then press Enter:"
read -r GCP_STORAGE_KEY

# Write the key to GCP-Key.json
echo "$GCP_STORAGE_KEY" > GCP-Key.json
echo "GCP-Key.json created successfully."
