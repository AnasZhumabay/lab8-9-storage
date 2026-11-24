"""
tests/test_storage.py — Lab 8-9, Task 3
Integration tests against the local MinIO container (docker compose up -d minio).

Run from the repo root:  pytest -v
"""

import hashlib
import os
import sys
import uuid

import boto3
import pytest
from botocore.client import Config
from botocore.exceptions import ClientError, EndpointConnectionError

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import s3_client  # noqa: E402

BUCKET = s3_client.BUCKET_NAME


@pytest.fixture(scope="module")
def s3():
    client = boto3.client(
        "s3",
        endpoint_url=s3_client.ENDPOINT_URL,
        aws_access_key_id=s3_client.ACCESS_KEY,
        aws_secret_access_key=s3_client.SECRET_KEY,
        config=Config(signature_version="s3v4", connect_timeout=3,
                      retries={"max_attempts": 1}),
        region_name="us-east-1",
    )
    try:
        client.list_buckets()
    except EndpointConnectionError:
        pytest.skip(f"MinIO not reachable at {s3_client.ENDPOINT_URL} — run `docker compose up -d minio`")
    s3_client.init_storage()
    return client


@pytest.fixture(scope="module")
def prefix(s3):
    """Unique key prefix per test run; everything under it is deleted afterwards."""
    p = f"pytest-{uuid.uuid4().hex[:8]}/"
    yield p
    s3_client.delete_prefix(p)


# 1 ------------------------------------------------------------------------- #
def test_s3_bucket_existence(s3):
    buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    assert BUCKET in buckets


# 2 ------------------------------------------------------------------------- #
def test_s3_object_upload_and_read(s3, prefix):
    key = f"{prefix}test_dir/sample.txt"
    content = b"Stream Data Content"
    s3.put_object(Bucket=BUCKET, Key=key, Body=content)
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    assert obj["Body"].read() == content
    assert obj["ContentLength"] == len(content)


# 3 ------------------------------------------------------------------------- #
def test_custom_metadata_roundtrip_via_head(s3, prefix, tmp_path):
    src = tmp_path / "meta.txt"
    src.write_bytes(b"metadata payload")
    key = f"{prefix}meta/meta.txt"
    meta = {"course": "CSS4004", "lab": "8-9", "Owner": "Student"}

    elapsed = s3_client.upload_object(key, str(src), metadata=meta)
    got = s3_client.read_object_metadata(key)

    assert elapsed > 0
    # S3 returns user-metadata keys lower-cased (x-amz-meta-* headers)
    assert got == {k.lower(): v for k, v in meta.items()}


# 4 ------------------------------------------------------------------------- #
def test_overwrite_replaces_whole_object(s3, prefix):
    """Objects are immutable: a PUT to an existing key replaces it entirely."""
    key = f"{prefix}overwrite.txt"
    etag_v1 = s3_client.put_bytes(key, b"version-1-long-original-content")
    etag_v2 = s3_client.put_bytes(key, b"v2")
    assert etag_v1 != etag_v2
    assert s3_client.read_object(key) == b"v2"  # no leftover bytes from v1


# 5 ------------------------------------------------------------------------- #
def test_flat_namespace_prefix_listing(s3, prefix):
    """'Directories' are only key prefixes in a flat namespace."""
    # Own sub-prefix: the module-scoped `prefix` is shared with other tests
    # (test_dir/, meta/, ...), so listing it directly would depend on test order.
    ns = f"{prefix}flat-namespace/"
    keys = [f"{ns}logs/2026/a.log", f"{ns}logs/2026/b.log", f"{ns}images/c.png"]
    for k in keys:
        s3_client.put_bytes(k, b"x")

    assert sorted(s3_client.list_keys(f"{ns}logs/")) == sorted(keys[:2])

    # A delimiter makes S3 *simulate* folders as CommonPrefixes
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=ns, Delimiter="/")
    folders = sorted(cp["Prefix"] for cp in resp.get("CommonPrefixes", []))
    assert folders == [f"{ns}images/", f"{ns}logs/"]


# 6 ------------------------------------------------------------------------- #
def test_delete_then_head_returns_404(s3, prefix):
    key = f"{prefix}to_delete.txt"
    s3_client.put_bytes(key, b"bye")
    assert s3_client.object_exists(key)

    s3_client.delete_object(key)

    assert not s3_client.object_exists(key)
    with pytest.raises(ClientError) as exc:
        s3.head_object(Bucket=BUCKET, Key=key)
    assert exc.value.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound")


# 7 ------------------------------------------------------------------------- #
def test_read_after_write_consistency(s3, prefix):
    """Bounded staleness probe: the new version must be visible (MinIO is strongly consistent)."""
    key = f"{prefix}consistency.txt"
    s3_client.put_bytes(key, b"old")
    for i in range(5):
        new = f"new-{i}-".encode() + os.urandom(8)
        window = s3_client.measure_read_after_write(key, new, timeout=2.0)
        assert window < 2.0


# 8 ------------------------------------------------------------------------- #
def test_multipart_upload_integrity(s3, prefix, tmp_path):
    """A 20 MB file goes over the 8 MB threshold, so upload_file uses multipart."""
    src = tmp_path / "big.bin"
    payload = os.urandom(20 * 1024 * 1024)
    src.write_bytes(payload)
    key = f"{prefix}big/big.bin"

    s3_client.upload_object(key, str(src))
    head = s3.head_object(Bucket=BUCKET, Key=key)

    assert head["ContentLength"] == len(payload)
    assert "-" in head["ETag"]  # multipart ETag format: "<md5-of-md5s>-<part count>"

    dst = tmp_path / "big_downloaded.bin"
    s3_client.download_object(key, str(dst))
    assert hashlib.sha256(dst.read_bytes()).digest() == hashlib.sha256(payload).digest()
