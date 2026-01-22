from google.cloud import storage
import os
import gzip
import shutil

def download_and_extract_gcs_file(storage_url, destination_dir):
    if not storage_url.startswith("gs://"):
        raise ValueError("Invalid GCS URL. It should start with 'gs://'.")

    key_file_path = "GCP-Key.json"
    if not os.path.exists(key_file_path):
        raise FileNotFoundError(f"Key file not found: {key_file_path}")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_file_path

    parts = storage_url[5:].split("/", 1)
    if len(parts) != 2:
        raise ValueError("Invalid GCS URL format. It should be 'gs://bucket_name/object_name'.")

    bucket_name, blob_name = parts
    gzipped_file_path = os.path.join(destination_dir, os.path.basename(blob_name))
    extracted_file_path = os.path.join(destination_dir, os.path.splitext(os.path.basename(blob_name))[0])

    os.makedirs(destination_dir, exist_ok=True)

    client = storage.Client()

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(gzipped_file_path)
    print(f"File downloaded to: {gzipped_file_path}")

    with gzip.open(gzipped_file_path, 'rb') as gzipped_file:
        with open(extracted_file_path, 'wb') as extracted_file:
            shutil.copyfileobj(gzipped_file, extracted_file)

    print(f"File extracted to: {extracted_file_path}")

    os.remove(gzipped_file_path)
    print(f"Temporary gzipped file deleted: {gzipped_file_path}")

if __name__ == "__main__":
    STORAGE_URL = "gs://ml-pipeline-az/RawData.csv.gz"
    DESTINATION_DIR = "../"
    download_and_extract_gcs_file(STORAGE_URL, DESTINATION_DIR)
