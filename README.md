<h1 align="center">Streaming ML Modle Prediction Streaming ingestion Pipeline for Model Prediction and Monitoring</h1>
<h3 align="center">This project looks at a simulated streaming ingestion pipeline, transforms raw data to be fed into a machine learning model to make predictions on customer churn and monitorins performance of models using NannyML</h3>

<h3 align="left">Connect with me:</h3>
<p align="left">
</p>

<h3 align="left">Languages and Tools:</h3>
<p align="left"> 
  <a href="https://www.docker.com/" target="_blank" rel="noreferrer"> 
    <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/docker/docker-original-wordmark.svg" alt="docker" width="40" height="40"/> 
  </a> 
  <a href="https://git-scm.com/" target="_blank" rel="noreferrer"> 
    <img src="https://www.vectorlogo.zone/logos/git-scm/git-scm-icon.svg" alt="git" width="40" height="40"/> 
  </a> 
  <a href="https://www.linux.org/" target="_blank" rel="noreferrer"> 
    <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/linux/linux-original.svg" alt="linux" width="40" height="40"/> 
  </a> 
  <a href="https://www.mysql.com/" target="_blank" rel="noreferrer"> 
    <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/mysql/mysql-original-wordmark.svg" alt="mysql" width="40" height="40"/> 
  </a> 
  <a href="https://pandas.pydata.org/" target="_blank" rel="noreferrer"> 
    <img src="https://raw.githubusercontent.com/devicons/devicon/2ae2a900d2f041da66e950e4d48052658d850630/icons/pandas/pandas-original.svg" alt="pandas" width="40" height="40"/> 
  </a> 
  <a href="https://www.python.org" target="_blank" rel="noreferrer"> 
    <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/python/python-original.svg" alt="python" width="40" height="40"/> 
  </a> 
  <a href="https://scikit-learn.org/" target="_blank" rel="noreferrer"> 
    <img src="https://upload.wikimedia.org/wikipedia/commons/0/05/Scikit_learn_logo_small.svg" alt="scikit_learn" width="40" height="40"/> 
  </a> 
  <a href="https://seaborn.pydata.org/" target="_blank" rel="noreferrer"> 
    <img src="https://seaborn.pydata.org/_images/logo-mark-lightbg.svg" alt="seaborn" width="40" height="40"/> 
  </a> 
  <a href="https://spark.apache.org/" target="_blank" rel="noreferrer"> 
    <img src="https://spark.apache.org/images/spark-logo-trademark.png" alt="pyspark" width="40" height="40"/> 
  </a> 
  <a href="https://nannyml.com/" target="_blank" rel="noreferrer"> 
    <img src="https://nannyml.com/static/images/logo-white.svg" alt="nannyml" width="40" height="40"/> 
  </a> 
  <a href="https://docs.docker.com/compose/" target="_blank" rel="noreferrer"> 
    <img src="https://www.docker.com/wp-content/uploads/2022/03/Moby-logo.png" alt="docker compose" width="40" height="40"/> 
  </a>
</p>


---
# STEP 0: Downloading Data Files from Google Cloud Storage

GitHub Workflow is used to generate an artifact containing the Google Cloud Storage Key allowing for access to the cloud storage bucket that retrieves the raw data used to populate the _RawData_ database docker instance. Once the key has been made available the script _DownloadDBFile.py_ retrieves the raw data from cloud bucket.
## Steps to run _DownloadDBFile.py_
### 1. Generate the GCP Key Artifact

1. Go to **Actions** tab in the GitHub repository.

2. Select Setup GCP Key workflow.

3 Click **Run workflow**.

After completion, this will produce an artifact named ```GCP-Key.json.zip```.

### 2. Download and Extract the Key

1. Download the ```GCP-Key.json.zip``` artifact.

2. Place it in the ```flaskapp/DataFile``` directory.

3. Unzip the file
```unzip GCP-Key.json.zip```

### 3. Install Dependencies

Make sure you have all required Python packages installed. Run:
```pip install google-cloud-storage```
### 4. Run the Script Locally

Finally, run the script to download the database file:
```python DownloadDBFile.py``

This will create the RawData.csv file in the _flaskapp_ directory required to populate the docker MySQL instance.

### directory structure
```
flaskapp/ 
|--- DataFile/ 
|   └── DownloadDBFile.py 
    └── GCP-Key.json
| RawData.csv
```
---

![Docker Architecture](PlantUMLDiagrams/Docker-Architecture.png)
# Step 1: Creating Shared Network and Run the Flaskapp Multi-Container Setup

## 1.1 Create the Shared Docker Network

1. **Create the network** (if not already created) by running:

```docker create network shared-network```

2. **(Optional) Manually connect containers** to the network (if needed):

```docker network connect shared-network flaskapp-flaskapp-db-1```

```docker network connect shared-network flaskapp-flaskapp-app-1```
## 1.2 Build and Run Docker Containers

from the ```/flaskapp``` directory run:

```
docker compose up --build
```

This command: 

- Spins up two containers:
    - **flaskapp-flaskapp-db-1** (MySQL instance)
    - **flaskapp-flaskapp-app-1** (Flask-based application)
 
- Runs the ```DataBase.py``` script on the first run to *populate the MySQL instance* with initial data, creating ```DP_CDR_Data``` table in the ```RawData``` database.

- Launches a **streaming API** in the ```flaskapp-flaskapp-app-1``` container that simulates Call Detail Records (CDR) based on the sample in the ```DP_CDR_Data``` table

  ## 1.3 Run the Streaming Script

  Once the database is populated, a **streaming ingestion simulator** is exposed via a Flask REST API.

  ### API Parameters
 - _num_baches_: Number of iterations.
 - _batch_size_: Records per iteration.
 - _interval_: Wait time between batches.

### Usage Example
   
```
curl -X POST http://127.0.0.1:5000/start_stream \ 
-H "Content-Type: application/json" \ 
-d '{"batch_size": 1000, "num_batches": 5, "interval": 10}' 
```
***

This will send 5 batches of 1000 records each, waiting 10 seconds between batches.

> **NOTE**: Keep this docker instance running, as various docker instances in the pipeline are connected to **flaskapp-flaskapp-db-1**

** 1.4 MySQL Procedure in Docker Container**

A MySQL **procedure** is part of the Docker volumes when **flaskapp-flaskapp-db-1** is created. This procedure summarizes the data from ```DP_CDR_Data```

This can be run once the **streaming ingestion** has completed to get a summarized view of the data inserted. 

*** Running MySQL Procedure. *** 

1. **Access the MySQL container**:
```bash
docker exec -it flaskapp-flaskapp-db-1 mysql -u root -p
```

Enter the password whe prompted.

2. **Execute the procedure** once inside the MySQL shell:

```
USE RawData;
CALL GetDailyCDRDataBatch(CURDATE() - INTERVAL <day_value> DAY);
```

- This command aggregate summary data of user records.

![Exploratory Data Analysis](PlantUMLDiagrams/PySparkEDAScript.png)
# Step 2: Exploratotry Data Analysis
From the ```/ExploratoryDataAnalysis``` direcory, you can build and run the Docker container that executes the exploratory data analysis script.

```
docker compose up --build
```

This command:
- Spin up a container that runs the *EDA.py* and *main.py* scripts.
- Generates correlation plots and analysis results in the ```/output``` directory (as defined by Docker Volumes)

## 2.1 Configure and Run the Analysis

1. *Specify the date parameters* in ```query_params.json```

```
{
  "start_date": "<date_value>",
  "end_date": "<date_value>"
}
```
These dates determine which records are selected from the ```DP_CDR_Data``` table.

2. **Run the EDA script** by executing:

```
docker compose up --build
```

This script will generate **correlation plots** and other analysis outputs, which are stored in the ```/output``` directory.

These correlations inform the feature generation for the subsequent machine learning tasks.
***

![Data Preparation](PlantUMLDiagrams/PySparkAnalysis.png)

![Machine Learning Model](PlantUMLDiagrams/pySparkModel.png)
# Step 3: Data Preparation and Machine Learning Model
A PySpark workflow is used to:

1. **Retrieve and transform** data from the MySQL Docker instance.

2. **Store** cleaned data and generate a Parquet file for validation or additional exploration.

3. **Train** a Random Forest Classifier to predict customer churn, storing the results in the ```RawData``` database

## 3.1 Run the Multi-Container Setup

From the ```/pyspark``` directory, run:

```
docker compose up --build
```

This command will:
- Launch the ```pyspark-pyspark-analysis-1``` container to **transform and clean** the data for modeling.
- **Automatically** run the ```pyspark-pyspark-model-1``` container **after** the analysis script completes, training a Random Forest Classifier to generate churn predictions.
- Store model predicions in the ```RawData``` database under ```model_predictions```

> **Note**: Your Docker environment should have at least 16GB of memory to run the model.

## 3.2 Configure Date Ranges
Edit the ```config.json``` file to specify date ranges for both data transformation and model creation:

```
{
  "start_date": "<date_value>", # PySparkAnalysis Parameters
  "end_date": "<date_value>"

  "processed_start": "<date_value>", #pySparkModel Parameters
  "processed_end": "<date_value>"
}

```

These data values control which records are selected from the database and how they are processed prior to modelling
***

![Model Monitoring](PlantUMLDiagrams/Model_Monitoring.png)
# Step 4: Model Performance Monitoring

In this stage, model metrics are evaluated to measure F1-score, Accuracy, and ROC-AUC indicators. Tracking deviations in these metrics helps to identify potential:
- Data Drift
- Model Drift
- Model Degradation

Key metrics monitored include:
- **F1 Score**
- **ROC-AUC Curve**
- **Accuracy**

## Run Monitoring Service
From the ```/ModelMonitoring``` directory, build and launch the service:

```
docker compose up --build
```

This command start an application that hosts a REST API for triggering the model monitoring scripts. The generated model metrics are stored in the ```output``` directory.

### Triggering the Monitoring Script
You can **manually** trigger the monitoring script by calling:

```
curl http://localhost:5001/run-monitoring
```

However, a **polling script** is also available to automatically check for new entries in the ```model_predictions``` table. When new predictions are detected, the script runs automatically to update the monitoring metrics. 
