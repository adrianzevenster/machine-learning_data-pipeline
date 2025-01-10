from google.cloud import storage
import os
import gzip
import shutil

def download_and_extract_gcs_file(storage_url, destination_dir):
    """
    Downloads a gzipped file from Google Cloud Storage and extracts it as a CSV file.

    Args:
        storage_url (str): GCS URL in the format 'gs://bucket_name/object_name'.
        destination_dir (str): Local directory to save the file.
    """
    if not storage_url.startswith("gs://"):
        raise ValueError("Invalid GCS URL. It should start with 'gs://'.")

    # Load Google Cloud credentials directly from the JSON file
    key_file_path = "GCP-Key.json"  # Replace with the correct path to your GCP JSON key
    if not os.path.exists(key_file_path):
        raise FileNotFoundError(f"Key file not found: {key_file_path}")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_file_path

    # Parse bucket name and blob name from the URL
    parts = storage_url[5:].split("/", 1)
    if len(parts) != 2:
        raise ValueError("Invalid GCS URL format. It should be 'gs://bucket_name/object_name'.")

    bucket_name, blob_name = parts
    gzipped_file_path = os.path.join(destination_dir, os.path.basename(blob_name))
    extracted_file_path = os.path.join(destination_dir, os.path.splitext(os.path.basename(blob_name))[0])

    # Create the local directory if it doesn't exist
    os.makedirs(destination_dir, exist_ok=True)

    # Initialize the GCS client
    client = storage.Client()

    # Download the gzipped file
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(gzipped_file_path)
    print(f"File downloaded to: {gzipped_file_path}")

    # Extract the gzipped file
    with gzip.open(gzipped_file_path, 'rb') as gzipped_file:
        with open(extracted_file_path, 'wb') as extracted_file:
            shutil.copyfileobj(gzipped_file, extracted_file)

    print(f"File extracted to: {extracted_file_path}")

    # Optionally, delete the gzipped file after extraction
    os.remove(gzipped_file_path)
    print(f"Temporary gzipped file deleted: {gzipped_file_path}")

# Example usage
if __name__ == "__main__":
    STORAGE_URL = "gs://ml-pipeline-az/RawData.csv.gz"
    DESTINATION_DIR = "../flaskapp"
    download_and_extract_gcs_file(STORAGE_URL, DESTINATION_DIR)
