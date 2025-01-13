#!/bin/bash

# Check if GCP-Key.json already exists
if [ -f GCP-Key.json ]; then
  echo "GCP-Key.json already exists. Skipping setup."
  exit 0
fi

# Fetch the secret using GitHub CLI
echo "Fetching GCP key from GitHub Secrets..."
GCP_STORAGE_KEY=$(gh secret list | grep GCP_STORAGE_KEY)

if [ -z "$GCP_STORAGE_KEY" ]; then
  echo "Failed to retrieve GCP key from GitHub Secrets. Ensure GitHub CLI is authenticated."
  exit 1
fi

# Write the key to GCP-Key.json
echo "Creating GCP-Key.json..."
echo "$GCP_STORAGE_KEY" > ml-data-pipeline-az-b8c32d82371e.json
echo "GCP-Key.json created successfully."
