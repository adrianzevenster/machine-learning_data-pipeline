# Use the official Python image
FROM python:3.10

# Set the working directory
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . .

# Install Java, required for PySpark
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Install Python dependencies
RUN pip install --no-cache-dir -r pySparkEnv.txt

# Download the MySQL JDBC driver
ADD https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.30/mysql-connector-java-8.0.30.jar /app/mysql-connector-java-8.0.30.jar

# Set the SPARK_CLASSPATH environment variable to include the JDBC driver
ENV SPARK_CLASSPATH="/app/mysql-connector-java-8.0.30.jar"

# Command to run the script
CMD ["python", "Data_Cleaning.py"]
