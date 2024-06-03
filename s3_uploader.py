import boto3
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)

def upload_to_s3(bucket_name):
    """Upload all files in today's date directory to an S3 bucket."""
    # Get the current date in a simple format (YYYY-MM-DD)
    date_now = datetime.now().strftime('%Y-%m-%d')
    directory_path = os.path.join('StorageResults', date_now)

    if not os.path.exists(directory_path):
        logging.error(f"Directory {directory_path} does not exist.")
        return

    s3_client = boto3.client('s3')

    # Iterate through all files in the directory
    for file_name in os.listdir(directory_path):
        full_path = os.path.join(directory_path, file_name)
        try:
            s3_client.upload_file(full_path, bucket_name, f"{date_now}/{file_name}")
            logging.info(f"File {full_path} uploaded to {bucket_name} into folder {date_now}")
        except Exception as e:
            logging.error(f"Failed to upload {full_path} to {bucket_name}: {e}")

if __name__ == "__main__":
    bucket_name = 'my-greengrass-bucket-test'  # Specify the bucket name here
    upload_to_s3(bucket_name)