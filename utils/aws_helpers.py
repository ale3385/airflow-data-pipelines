import logging
import os

import boto3

logger = logging.getLogger(__name__)

ENV = os.getenv("ENVIRONMENT", "DEV").upper()
S3_BUCKET = os.getenv("S3_BUCKET", "alt-auction-products-data")


def get_aws_session():
    """Return a boto3 session using environment credentials."""
    return boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )


def list_objects_paginated(bucket, prefix, suffix=None):
    """List all S3 objects under a prefix, optionally filtering by suffix."""
    session = get_aws_session()
    s3 = session.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    keys = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if suffix is None or key.endswith(suffix):
                keys.append(key)

    logger.info("Found %d objects in s3://%s/%s", len(keys), bucket, prefix)
    return keys


def upload_file_to_s3(local_path, bucket, s3_key):
    """Upload a local file to S3."""
    session = get_aws_session()
    s3 = session.client("s3")
    s3.upload_file(local_path, bucket, s3_key)
    logger.info("Uploaded %s to s3://%s/%s", local_path, bucket, s3_key)


def delete_s3_prefix(bucket, prefix):
    """Delete all objects under an S3 prefix."""
    session = get_aws_session()
    s3 = session.resource("s3")
    bucket_resource = s3.Bucket(bucket)
    objects = list(bucket_resource.objects.filter(Prefix=prefix))

    if objects:
        bucket_resource.delete_objects(
            Delete={"Objects": [{"Key": obj.key} for obj in objects]}
        )
        logger.info("Deleted %d objects from s3://%s/%s", len(objects), bucket, prefix)
