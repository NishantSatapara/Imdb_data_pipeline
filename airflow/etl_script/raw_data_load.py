import os
import boto3
import kagglehub
from botocore.client import Config
import shutil
from pathlib import Path

# 1. Download locally first (Kagglehub requirement)
local_raw_dir = '/opt/airflow/data/'
bucket_name = 'imdb-bucket'

download_path = kagglehub.dataset_download(
    'ashirwadsangwan/imdb-dataset', 
    output_dir=local_raw_dir, 
    force_download=True
)
#2. Configure Boto3 client for MinIO
s3_client = boto3.client(
    's3',
    endpoint_url='http://minio:9000', # Your MinIO endpoint
    aws_access_key_id='lakehouse_admin',
    aws_secret_access_key='lakehouse_password',
    config=Config(signature_version='s3v4')
)
# Find the exact local TSV filename downloaded
tsv_files = [f for f in os.listdir(download_path) if f.endswith('.tsv')]

if not tsv_files:
    raise FileNotFoundError("No TSV file found in the downloaded archive.")

for tsv_file in tsv_files:
    
    local_tsv_path = os.path.join(download_path, tsv_file)
    minio_object_key = f"raw/{tsv_file}"
    s3_client.upload_file(local_tsv_path, bucket_name, minio_object_key)
    print(f"Successfully uploaded raw file to MinIO: s3://{bucket_name}/{minio_object_key}")
    os.remove(local_tsv_path)

# Define your target path
dir_path = Path(local_raw_dir)
# Delete the directory tree permanently
if dir_path.exists():
    for item in dir_path.iterdir():
        if item.is_dir():
            shutil.rmtree(item)  # Safe to delete subdirectories
        else:
            item.unlink()