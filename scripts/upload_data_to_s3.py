## Upload RAW data to S3 bucket
import os
import logging
from botocore.exceptions import *
from config.s3_client_config import get_s3_client, BUCKET_NAME

logging.basicConfig(level=logging.INFO)

# -----------------------------------------------------------------------
# Function to upload file to S3
# -----------------------------------------------------------------------
def upload_files_from_folder(local_folder="data", bucket_name=BUCKET_NAME):
    s3 = get_s3_client()
    if not os.path.exists(local_folder):
        logging.error("Local folder does not exist.")
        return
    try:
        for file_name in os.listdir(local_folder):
            if not file_name.lower().endswith(".csv"):
                continue
            
            local_path = os.path.join(local_folder, file_name)
            if os.path.isfile(local_path):
                s3_key = f"raw/{file_name}"
                s3.upload_file(
                    local_path,
                    bucket_name,
                    s3_key
                )
                logging.info(f"Upload successful: s3://{bucket_name}/{s3_key}")
    except FileNotFoundError:
        logging.error("File not found.")
    except NoCredentialsError:
        logging.error("AWS credentials not available.")
    except Exception as e:
        logging.error(f"Upload failed: {e}")
# -----------------------------------------------------------------------
# Function to list files in S3 bucket
# -----------------------------------------------------------------------
def list_files_in_bucket(bucket_name = BUCKET_NAME):
    s3 = get_s3_client()
    try:
        response = s3.list_objects_v2(Bucket = bucket_name)
        if 'Contents' in response:
            logging.info(f"Files in bucket '{bucket_name}':")
            for obj in response['Contents']:
                logging.info(f" - {obj['Key']}")
        else:
            logging.info(f"No files found in bucket '{bucket_name}'.")
    except Exception as e:
        logging.error(f"Failed to list files: {e}")
        
# -----------------------------------------------------------------------
# Main execution
# -----------------------------------------------------------------------
if __name__ == "__main__":
    upload_files_from_folder()
    list_files_in_bucket()