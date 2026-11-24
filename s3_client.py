"""
s3_client.py — Lab 8-9, Task 2
Programmatic object lifecycle against a local MinIO (S3-compatible) store via boto3.

Covers: bucket init, PUT (upload_file / put_object), GET, HEAD metadata,
LIST by prefix, DELETE, and a read-after-write (bounded staleness) probe.

Connection settings can be overridden with environment variables:
  S3_ENDPOINT (default http://localhost:9001), S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET
"""

import os
import time

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

ENDPOINT_URL = os.getenv("S3_ENDPOINT", "http://localhost:9001")
ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadminpassword")
BUCKET_NAME = os.getenv("S3_BUCKET", "distributed-course-bucket")


def make_client():
    """Build a boto3 S3 client pointed at MinIO (path-style addressing, SigV4)."""
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )


s3 = make_client()


# --------------------------------------------------------------------------- #
# Bucket
# --------------------------------------------------------------------------- #
def init_storage(bucket=BUCKET_NAME):
    """Create the bucket if it does not exist. Idempotent."""
    buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if bucket not in buckets:
        s3.create_bucket(Bucket=bucket)
    return bucket


# --------------------------------------------------------------------------- #
# PUT
# --------------------------------------------------------------------------- #
def upload_object(key_name, file_path, metadata=None, bucket=BUCKET_NAME):
    """Upload a local file. Returns elapsed seconds.

    upload_file switches to multipart upload automatically for large files
    (default threshold 8 MB). User metadata is sent as x-amz-meta-* headers.
    """
    metadata = metadata or {}  # avoid the shared mutable default argument
    start_time = time.perf_counter()
    s3.upload_file(
        Filename=file_path,
        Bucket=bucket,
        Key=key_name,
        ExtraArgs={"Metadata": metadata},
    )
    return time.perf_counter() - start_time


def put_bytes(key_name, data, metadata=None, bucket=BUCKET_NAME):
    """Single-request PUT of an in-memory payload. Returns the ETag."""
    resp = s3.put_object(Bucket=bucket, Key=key_name, Body=data, Metadata=metadata or {})
    return resp["ETag"]


# --------------------------------------------------------------------------- #
# GET / HEAD
# --------------------------------------------------------------------------- #
def read_object(key_name, bucket=BUCKET_NAME):
    """Return the full object body as bytes."""
    return s3.get_object(Bucket=bucket, Key=key_name)["Body"].read()


def download_object(key_name, file_path, bucket=BUCKET_NAME):
    """Download to a local file. Returns elapsed seconds."""
    start_time = time.perf_counter()
    s3.download_file(Bucket=bucket, Key=key_name, Filename=file_path)
    return time.perf_counter() - start_time


def read_object_metadata(key_name, bucket=BUCKET_NAME):
    """HEAD the object and return user metadata (keys come back lower-cased)."""
    response = s3.head_object(Bucket=bucket, Key=key_name)
    return response["Metadata"]


def object_exists(key_name, bucket=BUCKET_NAME):
    try:
        s3.head_object(Bucket=bucket, Key=key_name)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


# --------------------------------------------------------------------------- #
# LIST / DELETE
# --------------------------------------------------------------------------- #
def list_keys(prefix="", bucket=BUCKET_NAME):
    """List all keys under a prefix (paginates past 1000 keys).

    S3 has a flat namespace: "dir/" is just part of the key string, which is
    why listing is by prefix rather than by directory.
    """
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return keys


def delete_object(key_name, bucket=BUCKET_NAME):
    s3.delete_object(Bucket=bucket, Key=key_name)


def delete_prefix(prefix, bucket=BUCKET_NAME):
    """Delete every object under a prefix (batch delete, 1000 keys per call)."""
    keys = list_keys(prefix, bucket)
    for i in range(0, len(keys), 1000):
        chunk = [{"Key": k} for k in keys[i:i + 1000]]
        s3.delete_objects(Bucket=bucket, Delete={"Objects": chunk, "Quiet": True})
    return len(keys)


# --------------------------------------------------------------------------- #
# Bounded staleness / read-after-write probe
# --------------------------------------------------------------------------- #
def measure_read_after_write(key_name, data, timeout=5.0, poll_interval=0.01, bucket=BUCKET_NAME):
    """Overwrite `key_name` with `data`, then poll GET until the new bytes are seen.

    Returns seconds between PUT completion and the first GET returning the new
    version (the observed staleness window). MinIO and AWS S3 (since Dec 2020)
    both give strong read-after-write consistency, so this should be ~0: the
    very first GET already returns the new data.
    Raises TimeoutError if the new version is not visible within `timeout`.
    """
    s3.put_object(Bucket=bucket, Key=key_name, Body=data)
    written_at = time.perf_counter()
    while True:
        if read_object(key_name, bucket) == data:
            return time.perf_counter() - written_at
        if time.perf_counter() - written_at > timeout:
            raise TimeoutError(f"{key_name}: new version not visible after {timeout}s")
        time.sleep(poll_interval)


if __name__ == "__main__":
    # Quick demo: python s3_client.py
    import tempfile

    init_storage()
    with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
        f.write(b"Hello from CSS4004 Lab 8-9\n")
        path = f.name

    meta = {"course": "CSS4004", "lab": "8-9", "author": "student"}
    elapsed = upload_object("demo/hello.txt", path, metadata=meta)
    print(f"Uploaded demo/hello.txt in {elapsed * 1000:.1f} ms")
    print("HEAD metadata :", read_object_metadata("demo/hello.txt"))
    print("GET body      :", read_object("demo/hello.txt"))
    print("Keys in demo/ :", list_keys("demo/"))
    window = measure_read_after_write("demo/consistency.txt", b"v2-" + os.urandom(8))
    print(f"Read-after-write staleness window: {window * 1000:.2f} ms")
    os.remove(path)
