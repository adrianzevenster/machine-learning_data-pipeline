FROM python:3.9

RUN apt-get update && apt-get install -y default-jdk wget

RUN pip install pyspark nannyml pandas mysql-connector-python flask pytest

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="$PATH:$JAVA_HOME/bin"

RUN wget -P /app https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.25/mysql-connector-java-8.0.25.jar

# Copy Model_Monitoring.py to /app
COPY Model_Monitoring.py /app/Model_Monitoring.py
COPY polling_db.py /app/polling_db.py
COPY entrypoint.sh /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh


# Add this temporary RUN command to list files in /app
RUN ls -l /app


CMD ["/app/entrypoint.sh"]

