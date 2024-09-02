

FROM python:3.10

# Set the working directory
WORKDIR /home/adrian/1-PycharmProjects/Data_Engineering_Project/DLMSDSEDE02/

# Copy the requirements file into the working directory
COPY reg.txt .

# Update pip and install packages from the requirements file
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r reg.txt

# Copy the rest of your application's source code into the working directory
COPY . .

# Command to run your application
CMD ["python", "./DataBase.py"]
