#!/usr/bin/env bash
# Task 1 (part A): write the 100 MB payload into HDFS and capture fsck block layout.
# Usage: ./scripts/hdfs_setup.sh
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p logs

NN=hdfs_namenode
FILE=/user/student/data/sample_100m.bin
OUT=logs/hdfs_fsck_initial.log

echo "Waiting for HDFS to leave safe mode and report 2 live DataNodes..."
for _ in $(seq 1 60); do
  live=$(docker exec "$NN" hdfs dfsadmin -report 2>/dev/null | sed -nE 's/.*Live datanodes \(([0-9]+)\).*/\1/p' | head -n1)
  live=${live:-0}
  [[ "$live" -ge 2 ]] && break
  sleep 5
done
docker exec "$NN" hdfs dfsadmin -safemode wait

docker exec "$NN" hdfs dfs -mkdir -p /user/student/data
docker exec "$NN" dd if=/dev/urandom of=/tmp/sample_100m.bin bs=1M count=100 status=none
docker exec "$NN" hdfs dfs -put -f /tmp/sample_100m.bin /user/student/data/

# Optional: the same payload with a 32 MB block size, so fsck shows 4 blocks
# instead of 1 (100 MB fits inside a single default 128 MB block).
docker exec "$NN" hdfs dfs -D dfs.blocksize=33554432 -put -f /tmp/sample_100m.bin /user/student/data/sample_100m_32mblocks.bin

{
  echo "===== $(date -u +%FT%TZ)  hdfs dfsadmin -report ====="
  docker exec "$NN" hdfs dfsadmin -report
  echo
  echo "===== fsck: default block size (128 MB) ====="
  docker exec "$NN" hdfs fsck "$FILE" -files -blocks -locations
  echo
  echo "===== fsck: 32 MB block size ====="
  docker exec "$NN" hdfs fsck /user/student/data/sample_100m_32mblocks.bin -files -blocks -locations
} 2>&1 | tee "$OUT"

echo
echo "Saved -> $OUT   (screenshot this plus http://localhost:9870 -> Datanodes tab)"
