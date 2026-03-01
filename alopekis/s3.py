import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable, List

import boto3
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError

from .config import DATAFILE_BUCKET, WORKERS

logger = logging.getLogger("main")
logger.propagate = False

logging.getLogger("botocore").setLevel(logging.INFO)
logging.getLogger("boto3").setLevel(logging.INFO)
logging.getLogger("s3transfer").setLevel(logging.INFO)


def empty_bucket(bucket: str) -> None:
    """Remove all the objects from the specified bucket.

    Args:
        bucket (str): The name of the bucket to empty.
    """
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket)

    # Delete all objects in the bucket
    bucket.objects.all().delete()


def put_files(
    files: Iterable[str], bucket: str, extra_args: dict, root_dir=None
) -> List[tuple[str, int | None, str | None, bool]]:
    """Put the specified files in the specified S3 bucket.

    Args:
        files (Iterable[str]): The filenames to be uploaded.
        bucket (str): The target S3 bucket name.
        extra_args (dict): Dictionary of extra arguments to pass to the S3 call.
        root_dir (str): The root directory that all filenames are relative to

    Returns:
        List[tuple[str, int | None, str | None, bool]]: A list of tuples containing (file_path, file_size, SHA256, success) for each file
    """

    # Since this is I/O rather than CPU work, the number of workers can be higher
    workers = int(WORKERS) * 4

    # Create s3 client here for thread safety purposes
    s3_client = boto3.client(
        "s3", config=Config(max_pool_connections=workers, tcp_keepalive=True)
    )

    results = []
    count = 0
    progress = 1

    logger.info("Starting parallel upload")
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(put_file, s3_client, file, bucket, extra_args, root_dir)
            for file in files
        ]
        total = len(futures)
        logger.info(f"Upload queued for {total} files")

        # Pick a reasonable number at which to report progress based on then number of files to upload
        if total < 100:
            progess = 5
        elif 100 < total < 1000:
            progress = 50
        else:
            progress = 100

        for f in as_completed(futures):
            try:
                filename, size, checksum, result = f.result()
                results.append((filename, size, checksum, result))
                # Only count actually uploaded files
                if result:
                    count += 1
                if count % progress == 0:
                    logger.info(f"Uploaded {count}/{total} files")
            except TypeError as e:
                logger.error(f"{e} - {f}")
        if count < total:
            logger.error(
                f"Some files did not upload succesfully - pool finished with {count}/{total} files uploaded"
            )

    return results


def put_file(
    client: BaseClient, file: str, bucket: str, extra_args: dict, root_dir=None
) -> tuple[str, int | None, str | None, bool]:
    if root_dir:
        file_path = os.path.join(root_dir, file)
    else:
        file_path = file
    if not os.path.isfile(file_path):
        logger.warn(f"File {file_path} does not exist")
        return file, None, None, False
    try:
        length = os.path.getsize(file_path)
        response = client.put_object(
            Body=open(file_path, "rb"),
            Bucket=bucket,
            Key=file,
            ContentLength=length,
            **extra_args,
        )
        logger.debug(f"Uploaded {file_path} to {bucket}/{file}")
        return file, length, response["ChecksumSHA256"], True
    except ClientError as e:
        logger.error(f"Failed to upload {file_path}: {e}")
        return file, None, None, False


def put_status_file(status: bytes) -> None:
    s3_client = boto3.client("s3")
    try:
        s3_client.put_object(
            Body=status,
            Bucket=DATAFILE_BUCKET,
            Key="STATUS.json",
        )
        logger.info("Updated STATUS.json")
    except ClientError as e:
        logger.error(f"Failed to update STATUS.json: {e}")
