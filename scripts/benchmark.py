"""
benchmark.py — Lab 8-9, Task 4: read-latency comparison, HDFS vs S3/MinIO.

Both systems are read over HTTP from the same vantage point (a container on the
compose network), so the comparison is not skewed by JVM start-up (`hdfs dfs -cat`
launches a JVM per call, ~1-2 s) or by Docker's host port-forwarding.

  HDFS : WebHDFS REST  (NameNode :9870 -> 307 redirect -> DataNode :9864)
  S3   : boto3 GetObject against MinIO :9000

Run it with:
  docker compose --profile bench run --rm bench

Writes results/latency_results.csv (every sample) and results/latency_summary.md.
"""

import csv
import os
import statistics
import time

import boto3
import requests
from botocore.client import Config

HDFS_HTTP = os.getenv("HDFS_HTTP", "http://namenode:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")
HDFS_DIR = "/user/student/bench"

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
BUCKET = os.getenv("S3_BUCKET", "distributed-course-bucket")

SIZES = {"1KB": 1024, "1MB": 1024**2, "10MB": 10 * 1024**2, "100MB": 100 * 1024**2}
REPS = {"1KB": 30, "1MB": 20, "10MB": 10, "100MB": 5}
OUT_DIR = os.getenv("OUT_DIR", os.path.join(os.path.dirname(__file__), "..", "results"))

s3 = boto3.client(
    "s3", endpoint_url=S3_ENDPOINT,
    aws_access_key_id=os.getenv("S3_ACCESS_KEY", "minioadmin"),
    aws_secret_access_key=os.getenv("S3_SECRET_KEY", "minioadminpassword"),
    config=Config(signature_version="s3v4"), region_name="us-east-1",
)
http = requests.Session()


# ----------------------------------------------------------------- WebHDFS --
def webhdfs(path, op, **params):
    return f"{HDFS_HTTP}/webhdfs/v1{path}?op={op}&user.name={HDFS_USER}" + "".join(
        f"&{k}={v}" for k, v in params.items())


def hdfs_put(path, data):
    # Two-step create: the NameNode answers 307 with the DataNode to stream to.
    r = http.put(webhdfs(path, "CREATE", overwrite="true"), allow_redirects=False)
    r.raise_for_status()
    http.put(r.headers["Location"], data=data).raise_for_status()


def hdfs_get(path):
    r = http.get(webhdfs(path, "OPEN"))  # follows the redirect to a DataNode
    r.raise_for_status()
    return r.content


def hdfs_stat(path):
    http.get(webhdfs(path, "GETFILESTATUS")).raise_for_status()


# ---------------------------------------------------------------------- S3 --
def s3_get(key):
    return s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()


def s3_stat(key):
    s3.head_object(Bucket=BUCKET, Key=key)


# ------------------------------------------------------------------ helpers --
def timed(fn, *args):
    t = time.perf_counter()
    out = fn(*args)
    return time.perf_counter() - t, out


def summarize(samples):
    s = sorted(samples)
    p95 = s[min(len(s) - 1, int(round(0.95 * (len(s) - 1))))]
    return statistics.median(s), statistics.mean(s), p95


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    if BUCKET not in [b["Name"] for b in s3.list_buckets().get("Buckets", [])]:
        s3.create_bucket(Bucket=BUCKET)
    http.put(webhdfs(HDFS_DIR, "MKDIRS")).raise_for_status()

    rows = []
    for label, size in SIZES.items():
        payload = os.urandom(size)
        hpath, skey = f"{HDFS_DIR}/obj_{label}.bin", f"bench/obj_{label}.bin"
        print(f"[{label}] writing test objects...", flush=True)
        hdfs_put(hpath, payload)
        s3.put_object(Bucket=BUCKET, Key=skey, Body=payload)

        for system, read, stat, ref in (("HDFS", hdfs_get, hdfs_stat, hpath),
                                        ("S3", s3_get, s3_stat, skey)):
            assert read(ref) == payload, f"{system} returned corrupt data"  # also warms up
            for i in range(REPS[label]):
                dt, _ = timed(read, ref)
                rows.append([system, "read", label, size, i, dt])
            for i in range(REPS["1KB"]):
                dt, _ = timed(stat, ref)
                rows.append([system, "metadata", label, size, i, dt])
        print(f"[{label}] done", flush=True)

    csv_path = os.path.join(OUT_DIR, "latency_results.csv")
    with open(csv_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["system", "op", "size_label", "size_bytes", "rep", "seconds"])
        w.writerows(rows)

    lines = ["| Size | Op | System | Median (ms) | Mean (ms) | p95 (ms) | Throughput (MB/s) |",
             "|---|---|---|---:|---:|---:|---:|"]
    for label, size in SIZES.items():
        for op in ("read", "metadata"):
            if op == "metadata" and label != "1KB":
                continue  # metadata cost does not depend on object size
            for system in ("HDFS", "S3"):
                xs = [r[5] for r in rows if r[0] == system and r[1] == op and r[2] == label]
                med, mean, p95 = summarize(xs)
                tput = f"{(size / 1024**2) / med:.1f}" if op == "read" else "—"
                op_name = "GET" if op == "read" else ("GETFILESTATUS" if system == "HDFS" else "HEAD")
                lines.append(f"| {label} | {op_name} | {system} | {med*1000:.2f} | "
                             f"{mean*1000:.2f} | {p95*1000:.2f} | {tput} |")
    md_path = os.path.join(OUT_DIR, "latency_summary.md")
    with open(md_path, "w") as f:
        f.write("\n".join(lines) + "\n")
    print("\n".join(lines))
    print(f"\nSaved {csv_path} and {md_path}")


if __name__ == "__main__":
    main()
