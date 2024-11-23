<h1 align="center">Streaming ML Modle Prediction Streaming ingestion Pipeline for Model Prediction and Monitoring</h1>
<h3 align="center">This project looks at a simulated streaming ingestion pipeline, transforms raw data to be fed into a machine learning model to make predictions on customer churn and monitorins performance of models using NannyML</h3>

<h3 align="left">Connect with me:</h3>
<p align="left">
</p>

<h3 align="left">Languages and Tools:</h3>
<p align="left"> <a href="https://www.docker.com/" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/docker/docker-original-wordmark.svg" alt="docker" width="40" height="40"/> </a> <a href="https://git-scm.com/" target="_blank" rel="noreferrer"> <img src="https://www.vectorlogo.zone/logos/git-scm/git-scm-icon.svg" alt="git" width="40" height="40"/> </a> <a href="https://www.linux.org/" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/linux/linux-original.svg" alt="linux" width="40" height="40"/> </a> <a href="https://www.mysql.com/" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/mysql/mysql-original-wordmark.svg" alt="mysql" width="40" height="40"/> </a> <a href="https://pandas.pydata.org/" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/2ae2a900d2f041da66e950e4d48052658d850630/icons/pandas/pandas-original.svg" alt="pandas" width="40" height="40"/> </a> <a href="https://www.python.org" target="_blank" rel="noreferrer"> <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/python/python-original.svg" alt="python" width="40" height="40"/> </a> <a href="https://scikit-learn.org/" target="_blank" rel="noreferrer"> <img src="https://upload.wikimedia.org/wikipedia/commons/0/05/Scikit_learn_logo_small.svg" alt="scikit_learn" width="40" height="40"/> </a> <a href="https://seaborn.pydata.org/" target="_blank" rel="noreferrer"> <img src="https://seaborn.pydata.org/_images/logo-mark-lightbg.svg" alt="seaborn" width="40" height="40"/> </a> </p>
---
# STEP 0: Downloading Data Files from Google Cloud Storage

Head to the following directory and run python script
- Ensure that gloud bucket key has been added to variable path
- Run the script _DownloadDBFile.py_ and place it under the ```flaskapp``` directory
- - THis script fetches the RawData used to populated the docker database
### directory structure
```
flaskapp/ 
|--- DataFile/ 
|   └── DownloadDBFile.py 
    └── ml-data-pipeline-b8c32d82371e.json
```
---
# Step 1: Creating Shared Network and Running Flaskapp Docker Multi Container
Create a shared network using the the following command ```docker create shared-network```
-- It might be required to add 
From the /flaskapp directory run the command ```docker compose up --build```.
This will run the docker-compose and Dockerfile which created an instance ```flaskapp-flaskappp-db
