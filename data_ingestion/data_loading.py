import boto3

# Initialize the S3 client
s3_client = boto3.client('s3')

# Define the bucket name and folder path
bucket_name = 's3://input-data-capstone'
input_data_folder = 'input-data/'

# List of files to upload
files_to_upload = [
    'claims.json',
    'disease.csv',
    'subscriber.csv',
    'grpsubgrp.csv',
]

# Upload files to S3
for file_name in files_to_upload:
    s3_client.upload_file(file_name, bucket_name, input_data_folder + file_name)

print("Data uploaded to S3 successfully.")