import os
import logging
from typing import List, Iterator, Iterable
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

logger = logging.getLogger("main")
logger.propagate = False


def empty_bucket(bucket: str) -> None:
    """Remove all the objects from the specified bucket.

    Args:
        bucket (str): The name of the bucket to empty.
    """
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    
    # Delete all objects in the bucket
    bucket.objects.all().delete()


def put_files(files: Iterable[str], bucket: str, extra_args: dict, root_dir=None) -> List[tuple]:
    """Put the specified files in the specified S3 bucket.
    
    Args:
        files (List[str]): The filenames to be uploaded.
        bucket (str): The target S3 bucket name.
        extra_args (dict): Dictionary of extra arguments to pass to the S3 call.
        root_dir (str): The root directory that all filenames are relative to
        
    Returns:
        List[tuple]: A list of tuples containing (file_path, success, message) for each file
    """
    s3_client = boto3.client('s3')
    results = []
    
    for filename in files:
        if root_dir:
            file_path = os.path.join(root_dir, filename)
        else:
            file_path = filename
        if not os.path.isfile(file_path):
            results.append((file_path, False, "File not found"))
            continue
            
        try:
            s3_client.upload_file(
                Filename=file_path,
                Bucket=bucket,
                Key=filename,
                ExtraArgs=extra_args,
                Config=TransferConfig(max_concurrency=32)
            )
            results.append((file_path, True, "Successfully uploaded"))
            logger.debug(f"Uploaded {file_path} to {bucket}/{file_path}")
            
        except ClientError as e:
            error_msg = f"Failed to upload {file_path}: {str(e)}"
            logger.error(error_msg)
            results.append((file_path, False, error_msg))
    
    return results
