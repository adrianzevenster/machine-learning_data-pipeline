FROM python:3.9

# Install necessary system dependencies
RUN apt-get update && apt-get install -y default-jdk wget

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="$PATH:$JAVA_HOME/bin"

# Pin numpy to a compatible version (quote the version constraint)
RUN pip install "numpy<2.0"

# Install Python libraries
RUN pip install pyspark nannyml pandas mysql-connector-python flask pytest

# Download the MySQL JDBC connector
RUN wget -P /app https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.25/mysql-connector-java-8.0.25.jar

# Copy application files to /app
COPY Model_Monitoring.py /app/Model_Monitoring.py
COPY polling_db.py /app/polling_db.py
COPY entrypoint.sh /app/entrypoint.sh

# Ensure the entrypoint script is executable
RUN chmod +x /app/entrypoint.sh

# Optional: List files in /app for debugging purposes
RUN ls -l /app

# Specify the entrypoint
CMD ["/app/entrypoint.sh"]
