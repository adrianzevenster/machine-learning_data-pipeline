

FROM python:3.10

# Set the working directory
WORKDIR /app

# Copy the requirements file into the working directory
COPY stream_requirements.txt .

# Update pip and install packages from the requirements file
RUN pip install --upgrade pip
RUN pip install  -r stream_requirements.txt

# Copy the rest of your application's source code into the working directory
COPY . .

#RUN mkdir -p DataFileStream
# Command to run your application
CMD ["python", "./Stream_Script.py"]
