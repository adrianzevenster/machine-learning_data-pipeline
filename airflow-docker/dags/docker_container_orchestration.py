"""
Airflow DAG: local_dev_pipeline  – FULL VERSION (with PySpark tasks)

• Waits for MySQL (local-mysql) to be reachable.
• POSTs /start_stream to the Flask service.
• Runs EDA container that binds host output folder.
• Executes PySparkAnalysis and PySparkModel in sequence.

Prerequisites
-------------
1. Airflow Connection `flask_service` → HTTP http://flaskapp:5000
2. Airflow Variables:
   - `DATA_PIPELINE_ROOT`: absolute host path to project root.
   - `SPARK_PROJECT_ROOT`: absolute host path to PySpark folder (contains data/, output/, config.json).
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import socket
import os

from airflow import DAG
from airflow.models import Variable
from airflow.sensors.python import PythonSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# ─── Defaults ─────────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ─── Constants ────────────────────────────────────────────────────────────────
MYSQL_HOST = "local-mysql"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PWD  = "a?xBVq1!"
MYSQL_DB   = "RawData"

# Default network for Airflow stack (web/scheduler/worker/EDA)
DOCKER_NETWORK = os.getenv("AIRFLOW_DOCKER_NETWORK", "airflow-docker_airflow-network")

# Separate network where `local-mysql` also has alias `flaskapp-db`
PYSPARK_NETWORK = os.getenv("PYSPARK_NETWORK", "airflow-network")

# Host path for EDA output (mount into EDA container)
PROJECT_ROOT = Path(Variable.get("DATA_PIPELINE_ROOT"))
EDA_OUTPUT_HOST = PROJECT_ROOT / "ExploratoryDataAnalysis" / "output"
if not EDA_OUTPUT_HOST.is_absolute():
    raise ValueError(f"DATA_PIPELINE_ROOT must be absolute, got: {PROJECT_ROOT}")

# Host paths for PySpark tasks
SPARK_ROOT = Path(Variable.get("SPARK_PROJECT_ROOT", str(PROJECT_ROOT / "pySpark")))
SPARK_DATA_HOST = SPARK_ROOT / "data"
SPARK_OUTPUT_HOST = SPARK_ROOT / "output"         # mounted at /app/parquetFiles
SPARK_CONFIG_HOST = SPARK_ROOT / "config.json"

# ─── MySQL readiness callable ─────────────────────────────────────────────────
def _wait_for_mysql() -> bool:
    try:
        with socket.create_connection((MYSQL_HOST, MYSQL_PORT), timeout=2):
            return True
    except OSError:
        return False

# ─── DAG Definition ───────────────────────────────────────────────────────────
with DAG(
        dag_id="local_dev_pipeline",
        description="Orchestrate MySQL → Flask → EDA → PySpark",
        default_args=DEFAULT_ARGS,
        schedule_interval=None,
        start_date=datetime(2025, 1, 1),
        catchup=False,
) as dag:

    mysql_ready = PythonSensor(
        task_id="mysql_ready",
        python_callable=_wait_for_mysql,
        poke_interval=5,
        timeout=300,
        mode="poke",
    )

    start_stream = SimpleHttpOperator(
        task_id="start_flask_stream",
        http_conn_id="flask_service",
        endpoint="start_stream",
        method="POST",
        data='{"batch_size":1000,"num_batches":10,"interval":10}',
        headers={"Content-Type": "application/json"},
        retries=6,
        retry_delay=timedelta(seconds=30),
    )

    # EDA container (python-app:latest) on the Airflow stack network
    run_eda = DockerOperator(
        task_id="run_eda",
        image="python-app:latest",
        container_name="eda-python-app-{{ ts_nodash }}",
        auto_remove=True,
        docker_url="unix:///var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mount_tmp_dir=False,
        environment={
            "MYSQL_HOST": MYSQL_HOST,
            "MYSQL_USER": MYSQL_USER,
            "MYSQL_PASSWORD": MYSQL_PWD,
            "MYSQL_DATABASE": MYSQL_DB,
        },
        mounts=[Mount(source=str(EDA_OUTPUT_HOST), target="/app/output", type="bind")],
        command="python EDA.py",
    )

    pyspark_db_dns_check = DockerOperator(
        task_id="pyspark_db_dns_check",
        image="busybox:1.36",
        container_name="pyspark-dns-check-{{ ts_nodash }}",
        auto_remove=True,
        docker_url="unix:///var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mount_tmp_dir=False,
        environment={"MYSQL_HOST": MYSQL_HOST, "MYSQL_PORT": str(MYSQL_PORT)},
        command="sh -c 'nslookup local-mysql && nc -vz -w 2 local-mysql 3306 || (echo FAIL && exit 1)'",
    )


    # --- PySpark Analysis ---
pyspark_analysis = DockerOperator(
    task_id="pyspark_analysis",
    image="pyspark-app:latest",
    container_name="pyspark-analysis-{{ ts_nodash }}",
    auto_remove=True,
    docker_url="unix:///var/run/docker.sock",
    network_mode=DOCKER_NETWORK,
    mount_tmp_dir=False,
    environment={
        "MYSQL_HOST": MYSQL_HOST,   # "local-mysql"
        "MYSQL_USER": MYSQL_USER,
        "MYSQL_PASSWORD": MYSQL_PWD,
        "MYSQL_DATABASE": MYSQL_DB,
    },
    mounts=[
        Mount(source=str(SPARK_DATA_HOST),   target="/app/data",         type="bind"),
        Mount(source=str(SPARK_OUTPUT_HOST), target="/app/parquetFiles", type="bind"),
        Mount(source=str(SPARK_CONFIG_HOST), target="/app/config.json",  type="bind"),
        Mount(source=str(SPARK_ROOT / "PySparkAnalysis.py"), target="/app/PySparkAnalysis.py", type="bind"),
        Mount(source=str(SPARK_ROOT / "pySparkModel.py"),    target="/app/pySparkModel.py",    type="bind", read_only=True),
    ],
    command=r"""
sh -euxc '
# 1) Patch /app/config.json host + JDBC
python - << "PY"
import os, json, re, socket, time, sys
HOST="{}"
CFG="/app/config.json"

def fix_jdbc(s): return re.sub(r"(?<=jdbc:mysql://)([^:/?#]+)", HOST, s)
def walk(o):
    if isinstance(o, dict):
        for k,v in list(o.items()):
            if isinstance(v,(dict,list)): walk(v)
            elif isinstance(v,str):
                v=v.replace("flaskapp-db", HOST)
                v=fix_jdbc(v)
                o[k]=v
        for k in ("host","hostname","db_host","mysql_host"):
            if k in o: o[k]=HOST
        for k in ("url","jdbc_url","jdbcUrl","connection","connection_string"):
            if k in o and isinstance(o[k],str): o[k]=fix_jdbc(o[k])
    elif isinstance(o,list):
        for i,v in enumerate(o):
            if isinstance(v,(dict,list)): walk(v)
            elif isinstance(v,str):
                v=v.replace("flaskapp-db", HOST)
                v=fix_jdbc(v)
                o[i]=v

try:
    with open(CFG) as f: cfg=json.load(f)
    walk(cfg)
    with open(CFG,"w") as f: json.dump(cfg,f)
    print("Patched config.json -> host =", HOST)
except Exception as e:
    print("config.json patch skipped:", e)

# quick TCP check
for i in range(1,6):
    try:
        with socket.create_connection((HOST,3306),timeout=2):
            print("MySQL TCP reachable at", HOST, "on attempt", i); break
    except Exception as e:
        print("Wait for MySQL:", e); time.sleep(2)
else:
    print("FATAL: cannot reach MySQL at", HOST); sys.exit(2)
PY
'""".format(MYSQL_HOST) + r"""
# 2) Copy /app -> /tmp/app-run and rewrite any leftover flaskapp-db / JDBC hosts
sh -euxc '
rm -rf /tmp/app-run && mkdir -p /tmp/app-run
cp -a /app/. /tmp/app-run/

python - << "PY"
import os, re, io
HOST=os.environ.get("MYSQL_HOST","local-mysql")
root="/tmp/app-run"
def fix(s):
    s=s.replace("flaskapp-db", HOST)
    s=re.sub(r"(?<=jdbc:mysql://)([^:/?#]+)", HOST, s)
    return s
exts={".py",".json",".conf",".properties",".ini",".txt",".yml",".yaml",".cfg"}
for dp,_,fns in os.walk(root):
    for fn in fns:
        p=os.path.join(dp,fn)
        _,ext=os.path.splitext(p)
        if ext not in exts: continue
        try:
            s=open(p,"r",errors="ignore").read()
        except: 
            continue
        ns=fix(s)
        if ns!=s:
            with open(p,"w") as w: w.write(ns)
            print("patched:", p)
PY

# 3) Sanity: fail fast if anything still mentions flaskapp-db
if grep -R "flaskapp-db" /tmp/app-run; then
  echo "FATAL: still found flaskapp-db after patch"; exit 2
fi

# 4) Run patched script
python /tmp/app-run/PySparkAnalysis.py
'
""",
)

# --- PySpark Model ---
pyspark_model = DockerOperator(
    task_id="pyspark_model",
    image="pyspark-app:latest",
    container_name="pyspark-model-{{ ts_nodash }}",
    auto_remove=True,
    docker_url="unix:///var/run/docker.sock",
    network_mode=DOCKER_NETWORK,
    mount_tmp_dir=False,
    environment={
        "MYSQL_HOST": MYSQL_HOST,
        "MYSQL_USER": MYSQL_USER,
        "MYSQL_PASSWORD": MYSQL_PWD,
        "MYSQL_DATABASE": MYSQL_DB,
    },
    mounts=[
        Mount(source=str(SPARK_OUTPUT_HOST), target="/app/parquetFiles", type="bind"),
        Mount(source=str(SPARK_CONFIG_HOST), target="/app/config.json",  type="bind"),
        Mount(source=str(SPARK_ROOT / "pySparkModel.py"), target="/app/pySparkModel.py", type="bind"),
    ],
    command=r"""
sh -euxc '
# patch config
python - << "PY"
import os, json, re
HOST=os.environ.get("MYSQL_HOST","local-mysql")
CFG="/app/config.json"
def fix_jdbc(s): return re.sub(r"(?<=jdbc:mysql://)([^:/?#]+)", HOST, s)
with open(CFG) as f: d=json.load(f)
def walk(o):
    if isinstance(o,dict):
        for k,v in list(o.items()):
            if isinstance(v,(dict,list)): walk(v)
            elif isinstance(v,str): o[k]=fix_jdbc(v.replace("flaskapp-db",HOST))
        for k in ("host","hostname","db_host","mysql_host"):
            if k in o: o[k]=HOST
        for k in ("url","jdbc_url","jdbcUrl","connection","connection_string"):
            if k in o and isinstance(o[k],str): o[k]=fix_jdbc(o[k])
    elif isinstance(o,list):
        for i,v in enumerate(o):
            if isinstance(v,(dict,list)): walk(v)
            elif isinstance(v,str): o[i]=fix_jdbc(v.replace("flaskapp-db",HOST))
walk(d)
with open(CFG,"w") as f: json.dump(d,f)
print("Patched config.json -> host =", HOST)
PY

# patch code into temp and run
rm -rf /tmp/app-run && mkdir -p /tmp/app-run
cp -a /app/. /tmp/app-run/
python - << "PY"
import os, re
HOST=os.environ.get("MYSQL_HOST","local-mysql")
for p in ("/tmp/app-run/pySparkModel.py",):
    try:
        s=open(p,"r",errors="ignore").read()
        ns=re.sub(r"(?<=jdbc:mysql://)([^:/?#]+)", HOST, s).replace("flaskapp-db",HOST)
        if ns!=s:
            open(p,"w").write(ns); print("patched:", p)
    except Exception as e:
        print("skip", p, e)
PY

if grep -R "flaskapp-db" /tmp/app-run; then
  echo "FATAL: still found flaskapp-db after patch"; exit 2
fi

python /tmp/app-run/pySparkModel.py
'
""",
)




mysql_ready >> start_stream >> run_eda >> pyspark_db_dns_check >> pyspark_analysis >> pyspark_model
