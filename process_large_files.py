
import os
import subprocess
from urllib.parse import urlparse
import concurrent.futures
from tqdm import tqdm

def parse_gcs_path(gcs_path):
    """Parses a GCS path into bucket name and prefix."""
    if not gcs_path.startswith("gs://"):
        raise ValueError(f"Invalid GCS path: {gcs_path}")
    
    parsed_url = urlparse(gcs_path)
    bucket_name = parsed_url.netloc
    prefix = parsed_url.path.lstrip('/')
    
    return bucket_name, prefix

def list_blobs(gcs_path):
    """Lists all the blobs in the bucket that begin with the prefix."""
    command = ["gsutil", "ls", gcs_path]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Failed to list blobs: {result.stderr.strip()}")
    blobs = result.stdout.splitlines()
    return blobs

def download_blob(blob_url, destination_file_name, num_slices=8):
    """Downloads a blob using gsutil with sliced download."""
    command = [
        "gsutil",
        "-o", "GSUtil:parallel_thread_count=1",
        "-o", f"GSUtil:sliced_object_download_max_components={num_slices}",
        "cp",
        blob_url,
        destination_file_name
    ]
    print(f"Running command: {' '.join(command)}")  # Debugging output
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Failed to download blob {blob_url}: {result.stderr.strip()}")

def download_all_files(gcs_path, destination_folder, num_slices=8):
    """Downloads all files from a GCS folder to a local directory."""
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    blobs = list_blobs(gcs_path)
    print(f"Found {len(blobs)} files to download.")

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_slices) as executor:
        futures = []
        for blob_url in blobs:
            destination_file_name = os.path.join(destination_folder, os.path.basename(blob_url))
            futures.append(executor.submit(download_blob, blob_url, destination_file_name, num_slices))

        # Progress bar
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Downloading files"):
            try:
                future.result()
            except Exception as e:
                print(f"Error downloading file: {e}")

if __name__ == "__main__":
    gcs_path = "gs://ai-drive-psg-2024-us-central1/scenario_1_large_file/"
    destination_folder = "/mnt/disks/local_disk_1"

    try:
        download_all_files(gcs_path, destination_folder)
    except Exception as e:
        print(f"An error occurred: {e}")
'''

import os
from google.cloud import storage
import concurrent.futures
from tqdm import tqdm
from urllib.parse import urlparse

def parse_gcs_path(gcs_path):
    """Parses a GCS path into bucket name and prefix."""
    if not gcs_path.startswith("gs://"):
        raise ValueError(f"Invalid GCS path: {gcs_path}")
    
    parsed_url = urlparse(gcs_path)
    bucket_name = parsed_url.netloc
    prefix = parsed_url.path.lstrip('/')
    
    return bucket_name, prefix

def list_blobs(bucket_name, prefix):
    """Lists all the blobs in the bucket that begin with the prefix."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    return blobs

def download_blob(blob, destination_file_name):
    """Downloads a blob using google-cloud-storage."""
    blob.download_to_filename(destination_file_name)

def download_all_files(gcs_path, destination_folder):
    """Downloads all files from a GCS folder to a local directory."""
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    bucket_name, prefix = parse_gcs_path(gcs_path)
    blobs = list_blobs(bucket_name, prefix)
    blob_list = list(blobs)
    print(f"Found {len(blob_list)} files to download.")

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = []
        for blob in blob_list:
            destination_file_name = os.path.join(destination_folder, os.path.basename(blob.name))
            futures.append(executor.submit(download_blob, blob, destination_file_name))

        # Progress bar
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Downloading files"):
            try:
                future.result()
            except Exception as e:
                print(f"Error downloading file: {e}")

if __name__ == "__main__":
    gcs_path = "gs://ai-drive-psg-2024-us-central1/scenario_1_large_file/"
    destination_folder = "/mnt/disks/local_disk_1"

    try:
        download_all_files(gcs_path, destination_folder)
    except Exception as e:
        print(f"An error occurred: {e}")
'''