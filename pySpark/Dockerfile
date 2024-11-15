# Use the official Python image
FROM python:3.8-slim

RUN apt-get update && apt-get install -y \
    default-jdk \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install pyspark pandas seaborn matplotlib mysql-connector-python

# Add the JDBC driver
ADD https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.25/mysql-connector-java-8.0.25.jar /opt/spark/jars/

# Copy the application files
COPY PySparkAnalysis.py /app/PySparkAnalysis.py
COPY pySparkModel.py /app/pySparkModel.py
COPY entrypoint.sh /app/entrypoint.sh

# Make entrypoint.sh executable
RUN chmod +x /app/entrypoint.sh

# Set the working directory
WORKDIR /app

# CMD as the default command to be overridden by Docker Compose if needed
CMD ["python", "/app/PySparkAnalysis.py"]
