#!/bin/bash


echo "Running PySparkAnalysis.py..."
python /app/PySparkAnalysis.py

# Check if PySparkAnalysis.py completed successfully
if [ $? -eq 0 ]; then
    echo "PySparkAnalysis.py completed successfully."

    # Check if the expected output files are present in the specified directory
    if [ "$(ls -A /app/parquetFiles 2>/dev/null)" ]; then
        echo "Parquet files generated. Running pySparkModel.py..."
        python /app/pySparkModel.py

        # Check if pySparkModel.py completed successfully
        if [ $? -eq 0 ]; then
            echo "pySparkModel.py completed successfully."
        else
            echo "pySparkModel.py encountered an error."
            exit 1
        fi
    else
        echo "No parquet files found. Skipping pySparkModel.py execution."
        exit 1
    fi
else
    echo "PySparkAnalysis.py failed. Exiting without running pySparkModel.py."
    exit 1
fi
