import gdown
import os
import shutil

# Define the Google Drive file ID and output path
file_id = "https://drive.google.com/file/d/1vzDWZB_C8WzUWrEjrd7AbZGkRKPCwnOo/view?usp=drive_link"  # Replace with your actual Google Drive file ID
output_path = "downloaded_file.csv"  # Specify the name for the downloaded file

# Construct the Google Drive download URL
url = f"{file_id}"

# Download the file
print("Downloading the CSV file from Google Drive...")
gdown.download(url, output_path, quiet=False)

# Define the target directory in your repository
target_directory = "~/PyCharmProjects/machine-learning_data-pipeline/flaskapp/DataFile"  # Replace with your target path

# Check if the directory exists; if not, create it
if not os.path.exists(target_directory):
    os.makedirs(target_directory)

# Move the downloaded file to the target directory
print(f"Moving the file to {target_directory}...")
shutil.move(output_path, os.path.join(target_directory, "large_database.csv"))

print("File downloaded and moved successfully.")
