import os
import gcsfs
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

def list_blobs(fs, bucket_name, prefix):
    """Lists all the blobs in the bucket that begin with the prefix."""
    return fs.ls(f"{bucket_name}/{prefix}")

def get_blob_size(fs, blob_path):
    """Get the size of a blob in bytes."""
    return fs.info(blob_path)['size']

def download_blob(fs, blob_path, destination_file_name):
    """Downloads a blob using gcsfs with streaming."""
    with fs.open(blob_path, 'rb') as source_file:
        with open(destination_file_name, 'wb') as dest_file:
            for chunk in iter(lambda: source_file.read(1024 * 1024), b""):
                dest_file.write(chunk)

def download_group(fs, bucket_name, blobs, destination_folder):
    """Downloads a group of blobs to the destination folder."""
    for blob in blobs:
        relative_path = os.path.relpath(blob, start=f'{bucket_name}/')
        destination_file_name = os.path.join(destination_folder, relative_path)
        os.makedirs(os.path.dirname(destination_file_name), exist_ok=True)
        download_blob(fs, blob, destination_file_name)

def group_blobs_by_size(fs, blobs, max_size_mb):
    """Groups blobs into chunks where each chunk does not exceed max_size_mb."""
    max_size_bytes = max_size_mb * 1024 * 1024
    grouped_blobs = []
    current_group = []
    current_size = 0

    for blob in blobs:
        blob_size = get_blob_size(fs, blob)
        if current_size + blob_size > max_size_bytes:
            grouped_blobs.append(current_group)
            current_group = []
            current_size = 0
        
        current_group.append(blob)
        current_size += blob_size

    if current_group:
        grouped_blobs.append(current_group)

    return grouped_blobs

def download_all_files(gcs_path, destination_folder, max_size_mb=100, num_threads=16):
    """Downloads all files from a GCS folder to a local directory."""
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    bucket_name, prefix = parse_gcs_path(gcs_path)
    fs = gcsfs.GCSFileSystem()
    blobs = list_blobs(fs, bucket_name, prefix)
    print(f"Found {len(blobs)} files to download.")

    grouped_blobs = group_blobs_by_size(fs, blobs, max_size_mb)
    print(f"Grouped files into {len(grouped_blobs)} chunks.")

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for group in grouped_blobs:
            futures.append(executor.submit(download_group, fs, bucket_name, group, destination_folder))

        # Progress bar
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Downloading files"):
            try:
                future.result()
            except Exception as e:
                print(f"Error downloading files: {e}")

if __name__ == "__main__":
    gcs_path = "gs://ai-drive-psg-2024-us-central1/scenario_4_very_small_files"
    destination_folder = "/mnt/disks/local_disk_1"

    try:
        download_all_files(gcs_path, destination_folder, max_size_mb=50, num_threads=16)  # Adjust num_threads as needed
    except Exception as e:
        print(f"An error occurred: {e}")




'''

import os
import gcsfs
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

def list_blobs(fs, bucket_name, prefix):
    """Lists all the blobs in the bucket that begin with the prefix."""
    return fs.ls(f"{bucket_name}/{prefix}")

def get_blob_size(fs, blob_path):
    """Get the size of a blob in bytes."""
    return fs.info(blob_path)['size']

def download_blob(fs, blob_path, destination_file_name):
    """Downloads a blob using gcsfs."""
    with fs.open(blob_path, 'rb') as source_file:
        with open(destination_file_name, 'wb') as dest_file:
            dest_file.write(source_file.read())

def download_group(fs, bucket_name, blobs, destination_folder):
    """Downloads a group of blobs to the destination folder."""
    for blob in blobs:
        relative_path = os.path.relpath(blob, start=f'{bucket_name}/')
        destination_file_name = os.path.join(destination_folder, relative_path)
        os.makedirs(os.path.dirname(destination_file_name), exist_ok=True)
        download_blob(fs, blob, destination_file_name)

def group_blobs_by_size(fs, blobs, max_size_mb):
    """Groups blobs into chunks where each chunk does not exceed max_size_mb."""
    max_size_bytes = max_size_mb * 1024 * 1024
    grouped_blobs = []
    current_group = []
    current_size = 0

    for blob in blobs:
        blob_size = get_blob_size(fs, blob)
        if current_size + blob_size > max_size_bytes:
            grouped_blobs.append(current_group)
            current_group = []
            current_size = 0
        
        current_group.append(blob)
        current_size += blob_size

    if current_group:
        grouped_blobs.append(current_group)

    return grouped_blobs

def download_all_files(gcs_path, destination_folder, max_size_mb=100, num_threads=16):
    """Downloads all files from a GCS folder to a local directory."""
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)

    bucket_name, prefix = parse_gcs_path(gcs_path)
    fs = gcsfs.GCSFileSystem()
    blobs = list_blobs(fs, bucket_name, prefix)
    print(f"Found {len(blobs)} files to download.")

    grouped_blobs = group_blobs_by_size(fs, blobs, max_size_mb)
    print(f"Grouped files into {len(grouped_blobs)} chunks.")

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for group in grouped_blobs:
            futures.append(executor.submit(download_group, fs, bucket_name, group, destination_folder))

        # Progress bar
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Downloading files"):
            try:
                future.result()
            except Exception as e:
                print(f"Error downloading files: {e}")

if __name__ == "__main__":
    gcs_path = "gs://ai-drive-psg-2024-us-central1/scenario_4_very_small_files"
    destination_folder = "/mnt/disks/local_disk_1"

    try:
        download_all_files(gcs_path, destination_folder, max_size_mb=50, num_threads=16)  # Adjust num_threads as needed
    except Exception as e:
        print(f"An error occurred: {e}")
'''