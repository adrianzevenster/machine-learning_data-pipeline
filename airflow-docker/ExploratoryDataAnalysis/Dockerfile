FROM python:3.9-slim

WORKDIR /app

COPY . /app

RUN apt-get update && \
    apt-get install -y libgl1 libgl1-mesa-glx && \
    rm -rf /var/lib/apt/lists/*


RUN pip install --no-cache-dir pandas numpy matplotlib seaborn scikit-learn pymysql mysql-connector-python

CMD ["python", "EDA.py"]