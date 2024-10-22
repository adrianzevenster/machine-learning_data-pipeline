# Use the official Python image as a base image
FROM python:3.9

RUN apt-get update && apt-get install -y default-mysql-client
# Set the working directory in the container
WORKDIR /app

# Copy the correct requirements file into the container
COPY streamrequirements.txt /app/

# Install the dependencies
RUN pip install --no-cache-dir -r streamrequirements.txt

# Copy the rest of the application code into the container
COPY . /app/

# Expose port 5000
EXPOSE 5000

# Command to run the Flask app
CMD ["python", "streamstream.py"]



