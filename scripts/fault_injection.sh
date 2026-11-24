#!/usr/bin/env bash
# Task 1 (part B): DataNode fault injection with timestamped logs.
#
#   1. Baseline: live nodes, fsck, checksum of the file
#   2. docker stop hdfs_datanode1
#   3. Poll until the NameNode declares it dead (~90 s with our hadoop.env)
#   4. Show Under-Replicated Blocks and confirm the file is STILL readable
#   5. docker start hdfs_datanode1, poll until replication is healthy again
#
# Usage: ./scripts/fault_injection.sh   (run hdfs_setup.sh first)
set -uo pipefail
cd "$(dirname "$0")/.."
mkdir -p logs

NN=hdfs_namenode
VICTIM=${VICTIM:-hdfs_datanode1}
FILE=/user/student/data/sample_100m.bin
LOG=logs/fault_injection_$(date +%Y%m%d_%H%M%S).log
T0=$(date +%s)

log()   { printf '[%s +%4ss] %s\n' "$(date +%T)" "$(( $(date +%s) - T0 ))" "$*" | tee -a "$LOG"; }
block() { tee -a "$LOG"; }

num()         { sed -nE "s/.*$1[[:space:]]*\(?([0-9]+).*/\1/p" | head -n1; }
live_count()  { docker exec "$NN" hdfs dfsadmin -report 2>/dev/null | num 'Live datanodes'; }
dead_count()  { local n; n=$(docker exec "$NN" hdfs dfsadmin -report 2>/dev/null | num 'Dead datanodes'); echo "${n:-0}"; }
under_repl()  { docker exec "$NN" hdfs fsck / 2>/dev/null | num 'Under-replicated blocks:'; }
fsck_status() { docker exec "$NN" hdfs fsck / 2>/dev/null | sed -nE "s/.*The filesystem under path '\/' is ([A-Z]+).*/\1/p"; }
checksum()    { docker exec "$NN" bash -c "hdfs dfs -cat $FILE 2>/dev/null | md5sum | cut -d' ' -f1"; }

log "===== PHASE 0: baseline ====="
log "live=$(live_count) dead=$(dead_count) under_replicated=$(under_repl) fsck=$(fsck_status)"
docker exec "$NN" hdfs fsck "$FILE" -files -blocks -locations 2>&1 | block
SUM_BEFORE=$(checksum)
log "md5 before fault: $SUM_BEFORE"

log "===== PHASE 1: docker stop $VICTIM ====="
docker stop "$VICTIM" | block
T_STOP=$(date +%s)

log "Polling until the NameNode marks the DataNode dead (no heartbeat)..."
while true; do
  l=$(live_count); d=$(dead_count); u=$(under_repl)
  log "live=$l dead=$d under_replicated=$u"
  [[ "$d" -ge 1 ]] && break
  (( $(date +%s) - T_STOP > 900 )) && { log "TIMEOUT waiting for dead-node detection"; break; }
  sleep 10
done
log "Dead-node detection took $(( $(date +%s) - T_STOP )) s after docker stop"

log "===== PHASE 2: degraded state ====="
sleep 5
docker exec "$NN" hdfs dfsadmin -report 2>&1 | block
docker exec "$NN" hdfs fsck "$FILE" -files -blocks -locations 2>&1 | block
SUM_DEGRADED=$(checksum)
log "md5 while degraded: $SUM_DEGRADED"
if [[ "$SUM_DEGRADED" == "$SUM_BEFORE" ]]; then
  log "RESULT: file fully readable from the surviving replica"
else
  log "RESULT: checksum MISMATCH / read failed"
fi
log "NOTE: with 2 DataNodes and replication=2 there is no third node to re-replicate to,"
log "      so blocks stay under-replicated until the failed node returns."
log ">>> Take your Web UI screenshot now (http://localhost:9870 -> Overview / Datanodes)."
sleep "${PAUSE_FOR_SCREENSHOT:-30}"

log "===== PHASE 3: recovery — docker start $VICTIM ====="
docker start "$VICTIM" | block
T_START=$(date +%s)
while true; do
  l=$(live_count); u=$(under_repl)
  log "live=$l under_replicated=$u"
  [[ "$l" -ge 2 && "$u" == "0" ]] && break
  (( $(date +%s) - T_START > 600 )) && { log "TIMEOUT waiting for recovery"; break; }
  sleep 10
done
log "Recovery to fully replicated took $(( $(date +%s) - T_START )) s after docker start"
docker exec "$NN" hdfs fsck "$FILE" -files -blocks -locations 2>&1 | block
log "md5 after recovery: $(checksum)"
log "Full log saved to $LOG"
