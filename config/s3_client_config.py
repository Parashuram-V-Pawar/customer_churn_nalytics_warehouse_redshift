import os
import boto3
from dotenv import load_dotenv

# -----------------------------------------------------------------------
# Load environment variables
# -----------------------------------------------------------------------
load_dotenv()

# -----------------------------------------------------------------------
# AWS configuration
# -----------------------------------------------------------------------
AWS_REGION = os.getenv("AWS_REGION")

# -----------------------------------------------------------------------
# client initialization functions
# -----------------------------------------------------------------------
s3_client = None
def get_s3_client():
    global s3_client
    if s3_client is None:
        s3_client = boto3.client("s3", region_name=AWS_REGION)
    return s3_client


redshift_client = None
def get_redshift_client():
    global redshift_client
    if redshift_client is None:
        redshift_client = boto3.client("redshift-data", region_name=AWS_REGION)
    return redshift_client