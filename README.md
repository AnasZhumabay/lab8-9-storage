# Lab 8-9 — HDFS & S3-Compatible Object Storage (MinIO)

CSS4004 – Distributed Systems

```
.
├── docker-compose.yml            # NameNode, 2 DataNodes, MinIO (+ on-demand `bench` runner)
├── docker-compose.datanode3.yml  # optional 3rd DataNode to observe real re-replication
├── hadoop.env                    # replication=2, fast dead-node detection, WebHDFS
├── s3_client.py                  # Task 2 – boto3 object lifecycle
├── tests/test_storage.py         # Task 3 – 8 pytest integration tests
├── scripts/
│   ├── hdfs_setup.sh             # Task 1 – 100 MB put + fsck  -> logs/
│   ├── fault_injection.sh        # Task 1 – stop/start datanode1 with timestamped log
│   └── benchmark.py              # Task 4 – HDFS vs S3 read latency -> results/
└── report/Lab_Report_Storage.md  # report draft (export to Lab_Report_Storage.pdf)
```

## Run order

```bash
# 0. Start the cluster (first pull takes a few minutes)
docker compose up -d
python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt

# Task 1 – HDFS blocks
./scripts/hdfs_setup.sh                 # then screenshot http://localhost:9870 (Datanodes tab)
./scripts/fault_injection.sh            # ~3-4 min; pauses 30 s for your Web UI screenshot
                                        # PAUSE_FOR_SCREENSHOT=120 ./scripts/fault_injection.sh for longer

# Task 2 – S3 client demo (creates bucket + object with custom metadata)
python s3_client.py                     # then screenshot http://localhost:9002 (minioadmin / minioadminpassword)

# Task 3 – tests
pytest                                  # 8 tests

# Task 4 – latency benchmark (runs inside the Docker network)
docker compose --profile bench run --rm bench
cat results/latency_summary.md

# Optional – watch HDFS actually re-replicate onto a 3rd node
docker compose -f docker-compose.yml -f docker-compose.datanode3.yml up -d datanode3
./scripts/fault_injection.sh
```

On Windows, run the `.sh` scripts from Git Bash or WSL.

## Building the PDF

Save screenshots into `report/screenshots/` with the file names used in the report
(`01_hdfs_datanodes.png` … `06_hdfs_under_replicated.png`), fill the `[ ]` placeholders, then:

```bash
cd report
pandoc Lab_Report_Storage.md -o Lab_Report_Storage.pdf        # needs a LaTeX engine
# or: pandoc Lab_Report_Storage.md -o Lab_Report_Storage.docx  -> open in Word / Docs -> export PDF
```

## Notes / deviations from the handout

- MinIO image changed from `minio/minio` to `cgr.dev/chainguard/minio:latest`: `minio/minio` can no longer be pulled
  (removed from Docker Hub; no usable tag on quay.io). Same command, ports and credentials.
- `tests/test_storage.py::test_flat_namespace_prefix_listing` uses its own sub-prefix, because the shared module-scoped
  prefix already contained folders created by other tests (the test depended on execution order).
- `version: '3'` removed from compose: Compose v2 ignores it and warns it is obsolete.
- `platform: linux/amd64` added because the bde2020 images are amd64-only (Apple Silicon runs them emulated).
- Named volumes added so data survives `docker compose restart`.
- `hadoop.env` shortens dead-node detection from about 10.5 min to about 90 s and enables WebHDFS for the benchmark.
- `upload_object(..., metadata={})` changed to `metadata=None`, so calls no longer share one mutable default dict.
- With only 2 DataNodes and replication = 2, a stopped node leaves blocks **under-replicated until it returns**,
  because there is no spare node to re-replicate to. This is expected and is discussed in report Section 3.

Tear down: `docker compose down` (add `-v` to delete all HDFS/MinIO data).
