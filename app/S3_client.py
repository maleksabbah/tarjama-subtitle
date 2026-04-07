"""
S3 Client
Works with MinIO locally and AWS S3 in production.
Same API — just change the endpoint URL.

Usage:
    from app.S3_client import upload_file, download_file, get_presigned_url

Install:
    pip install boto3
"""
import os
import boto3
from botocore.client import Config as BotoConfig

# Read from environment
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://127.0.0.1:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "asr-bucket")
S3_REGION = os.getenv("S3_REGION", "us-east-1")

# Create client
s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    region_name=S3_REGION,
    config=BotoConfig(signature_version="s3v4"),
)


def ensure_bucket():
    """Create the bucket if it doesn't exist."""
    try:
        s3.head_bucket(Bucket=S3_BUCKET)
    except Exception:
        try:
            s3.create_bucket(Bucket=S3_BUCKET)
            print(f"  [S3] Created bucket: {S3_BUCKET}")
        except Exception as e:
            print(f"  [S3] Bucket creation failed (may already exist): {e}")


def upload_file(local_path: str, s3_key: str) -> str:
    """Upload a local file to S3. Returns the S3 key."""
    s3.upload_file(local_path, S3_BUCKET, s3_key)
    return s3_key


def upload_bytes(data: bytes, s3_key: str, content_type: str = "application/octet-stream") -> str:
    """Upload raw bytes to S3."""
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=data, ContentType=content_type)
    return s3_key


def upload_json(data: str, s3_key: str) -> str:
    """Upload a JSON string to S3."""
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=data.encode("utf-8"), ContentType="application/json")
    return s3_key


def download_file(s3_key: str, local_path: str) -> str:
    """Download a file from S3 to local disk. Returns local path."""
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    s3.download_file(S3_BUCKET, s3_key, local_path)
    return local_path


def download_bytes(s3_key: str) -> bytes:
    """Download file contents as bytes."""
    response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
    return response["Body"].read()


def download_json(s3_key: str) -> str:
    """Download a JSON file as string."""
    data = download_bytes(s3_key)
    return data.decode("utf-8")


def get_presigned_url(s3_key: str, expires_in: int = 3600) -> str:
    """Generate a presigned URL for downloading a file."""
    return s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": S3_BUCKET, "Key": s3_key},
        ExpiresIn=expires_in,
    )


def get_presigned_upload_url(s3_key: str, expires_in: int = 3600) -> str:
    """Generate a presigned URL for uploading a file."""
    return s3.generate_presigned_url(
        "put_object",
        Params={"Bucket": S3_BUCKET, "Key": s3_key},
        ExpiresIn=expires_in,
    )


def delete_file(s3_key: str):
    """Delete a file from S3."""
    s3.delete_object(Bucket=S3_BUCKET, Key=s3_key)


def delete_prefix(prefix: str) -> int:
    """Delete all files with a given prefix (e.g., all files for a job)."""
    deleted = 0
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    if "Contents" in response:
        objects = [{"Key": obj["Key"]} for obj in response["Contents"]]
        s3.delete_objects(Bucket=S3_BUCKET, Delete={"Objects": objects})
        deleted = len(objects)
    return deleted


def list_files(prefix: str) -> list[dict]:
    """List all files with a given prefix."""
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    if "Contents" not in response:
        return []
    return [
        {
            "key": obj["Key"],
            "size": obj["Size"],
            "last_modified": obj["LastModified"].isoformat(),
        }
        for obj in response["Contents"]
    ]


def get_file_size(s3_key: str) -> int:
    """Get the size of a file in S3."""
    response = s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
    return response["ContentLength"]


def file_exists(s3_key: str) -> bool:
    """Check if a file exists in S3."""
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
        return True
    except Exception:
        return False