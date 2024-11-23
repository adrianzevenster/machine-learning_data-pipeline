# Use the official Python image as a base image
FROM python:3.9

RUN apt-get update && apt-get install -y default-mysql-client

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file
COPY streamrequirements.txt /app/

# Install the dependencies
RUN pip install --no-cache-dir -r streamrequirements.txt

# Copy the application code, including the database script and CSV
COPY . /app/

# Ensure RawData.csv is accessible
RUN chmod +r /app/RawData.csv

# Expose port 5000
EXPOSE 5000

# Command to run the database population script and Flask app
CMD ["sh", "-c", "python DataBase.py && python streamingestion.py"]