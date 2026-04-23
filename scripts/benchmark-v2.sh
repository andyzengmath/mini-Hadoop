#!/bin/bash
# mini-Hadoop vs Apache Hadoop — v2 Benchmark Suite
# New tests: L1-L3 (large file), F5-F10 (fault-tolerance extensions), D1 (sustained load)
#
# Usage:
#   ./scripts/benchmark-v2.sh all             # Run all v2 tests on both systems (~80 min)
#   ./scripts/benchmark-v2.sh mini-only       # mini-Hadoop only (~40 min)
#   ./scripts/benchmark-v2.sh hadoop-only     # Apache Hadoop only (~40 min)
#   ./scripts/benchmark-v2.sh quick           # smaller files + shorter D1 (~15 min)
#   ./scripts/benchmark-v2.sh mini-only L1    # only L1 on mini-Hadoop
#   ./scripts/benchmark-v2.sh all F6          # only F6 on both
#
# Prerequisites:
#   - Docker Desktop running
#   - make docker-build (for mini-Hadoop image)
#   - Apache Hadoop image will be pulled as needed

set -u

MODE="${1:-all}"
ONLY_TEST="${2:-}"
# Allow "quick" flag to be combined with "mini-only"/"hadoop-only" by threading via env var
QUICK="${QUICK:-0}"
[ "$MODE" = "quick" ] && QUICK=1
# When ONLY_TEST is "quick" (from old docs), treat as no filter + quick mode
if [ "$ONLY_TEST" = "quick" ]; then ONLY_TEST=""; QUICK=1; fi
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
RESULTS_DIR="$PROJECT_DIR/benchmark_v2_results_$(date +%Y%m%d_%H%M%S)"
MINI_COMPOSE="$PROJECT_DIR/docker/docker-compose.yml"
HADOOP_COMPOSE="$PROJECT_DIR/docker/hadoop/docker-compose.yml"

mkdir -p "$RESULTS_DIR"

echo "╔══════════════════════════════════════════════════════════╗"
echo "║  mini-Hadoop vs Apache Hadoop — v2 Benchmarks            ║"
echo "║  Mode: $MODE | Filter: ${ONLY_TEST:-<all>} | $(date '+%Y-%m-%d %H:%M')  ║"
echo "╚══════════════════════════════════════════════════════════╝"

# ─── Helpers ──────────────────────────────────────────────

ts_ms() { date +%s%3N 2>/dev/null || python3 -c "import time;print(int(time.time()*1000))"; }
log() { echo "$1" | tee -a "$RESULTS_DIR/all_results.txt"; }
tag_of() { [ "$1" = "mini" ] && echo "mini-Hadoop" || echo "Hadoop"; }
compose_of() { [ "$1" = "mini" ] && echo "$MINI_COMPOSE" || echo "$HADOOP_COMPOSE"; }

# Tee stderr to a log file rather than dropping it. Silent failures in v2.0 hid
# "not enough DataNodes" errors that would have pointed straight at the root cause.
mini_exec() { docker compose -f "$MINI_COMPOSE" exec -T client sh -c "$1" 2> >(tee -a "$RESULTS_DIR/mini_stderr.log" >&2); }
hadoop_exec() { docker compose -f "$HADOOP_COMPOSE" exec -T hadoop-client sh -c "$1" 2> >(tee -a "$RESULTS_DIR/hadoop_stderr.log" >&2); }
exec_fn() { [ "$1" = "mini" ] && mini_exec "$2" || hadoop_exec "$2"; }

put_fn() {
  if [ "$1" = "mini" ]; then mini_exec "hdfs put $2 $3"
  else hadoop_exec "hdfs dfs -put -f $2 $3"; fi
}
get_fn() {
  if [ "$1" = "mini" ]; then mini_exec "hdfs get $2 $3"
  else hadoop_exec "hdfs dfs -get $2 $3"; fi
}
mkdir_fn() {
  if [ "$1" = "mini" ]; then mini_exec "hdfs mkdir $2 2>/dev/null || true"
  else hadoop_exec "hdfs dfs -mkdir -p $2 2>/dev/null || true"; fi
}
worker_name() {
  if [ "$1" = "mini" ]; then echo "worker-$2"
  else echo "hadoop-worker-$2"; fi
}
namenode_name() {
  [ "$1" = "mini" ] && echo "namenode" || echo "hadoop-namenode"
}
rm_name() {
  [ "$1" = "mini" ] && echo "resourcemanager" || echo "hadoop-resourcemanager"
}

sha256_local() { exec_fn "$1" "sha256sum $2 2>/dev/null | awk '{print \$1}'" | tr -d '\r\n '; }

should_run() {
  # $1 = test name (e.g. "L1"). returns 0 if should run.
  [ -z "$ONLY_TEST" ] && return 0
  [ "$ONLY_TEST" = "$1" ] && return 0
  return 1
}

# ─── Cluster Management ──────────────────────────────────

start_mini() {
  log ">>> Starting mini-Hadoop cluster..."
  docker compose -f "$MINI_COMPOSE" up -d --build 2>&1 | tail -5
  sleep 12
  for i in $(seq 1 20); do
    local dn=$(mini_exec 'curl -s http://namenode:9100/metrics 2>/dev/null' | grep -o '"alive_datanodes":[0-9]*' | grep -o '[0-9]*' || true)
    [ "$dn" = "3" ] && break
    sleep 2
  done
  log "    mini-Hadoop ready"
}
stop_mini() { docker compose -f "$MINI_COMPOSE" down -v 2>&1 > /dev/null || true; }

start_hadoop() {
  log ">>> Starting Apache Hadoop cluster..."
  docker compose -f "$HADOOP_COMPOSE" up -d 2>&1 | tail -5
  for i in $(seq 1 60); do
    hadoop_exec "hdfs dfs -ls / 2>/dev/null" > /dev/null 2>&1 && break
    sleep 5
  done
  sleep 15  # give workers time to register
  log "    Apache Hadoop ready"
}
stop_hadoop() { docker compose -f "$HADOOP_COMPOSE" down -v 2>&1 > /dev/null || true; }

# ═══════════════════════════════════════════════════════════
# L1: Large File Write (500MB, 1GB)
# ═══════════════════════════════════════════════════════════
bench_L1_write() {
  should_run L1 || return 0
  local sys=$1; local tag=$(tag_of $sys)
  local sizes="500"; [ "$QUICK" = "1" ] && sizes="200"
  [ "$MODE" != "quick" ] && sizes="500 1024"
  log ""; log "=== L1: Large File Sequential Write ($tag) ==="
  mkdir_fn $sys "/bench/v2"
  for sz in $sizes; do
    local label="${sz}MB"; [ "$sz" = "1024" ] && label="1GB"
    # Generate file (use urandom — truly random, not compressible)
    exec_fn $sys "dd if=/dev/urandom bs=1M count=$sz of=/tmp/l1-${sz}m.dat 2>/dev/null"
    local orig_sha=$(sha256_local $sys "/tmp/l1-${sz}m.dat")
    local actual_bytes=$(exec_fn $sys "wc -c < /tmp/l1-${sz}m.dat" | tr -d ' \r\n\t')
    # 3 runs, discard first
    local times=""
    for r in 1 2 3; do
      exec_fn $sys "rm -f /tmp/sentinel-l1-${sz}-${r} 2>/dev/null; true" || true
      local s=$(ts_ms)
      put_fn $sys "/tmp/l1-${sz}m.dat" "/bench/v2/l1-${sz}m-r${r}.dat"
      local e=$(ts_ms)
      times="$times $((e-s))"
    done
    local avg=$(echo "$times" | awk '{s=0;n=0; for(i=2;i<=NF;i++){s+=$i;n++} if(n>0) printf "%d",s/n; else print 0}')
    local mbps="N/A"
    [ "$avg" -gt 0 ] && mbps=$(awk "BEGIN{printf \"%.1f\", $actual_bytes / 1048576 / ($avg / 1000)}")
    # Verify SHA of first written file
    exec_fn $sys "rm -f /tmp/l1-v-${sz}m.dat 2>/dev/null; true" || true
    get_fn $sys "/bench/v2/l1-${sz}m-r1.dat" "/tmp/l1-v-${sz}m.dat"
    local read_sha=$(sha256_local $sys "/tmp/l1-v-${sz}m.dat")
    local sha_status="FAIL"
    [ "$orig_sha" = "$read_sha" ] && [ -n "$orig_sha" ] && sha_status="PASS"
    log "  $label: avg=${avg}ms (${mbps} MB/s) [runs:$times] sha:$sha_status"
    echo "$label avg=${avg}ms mbps=${mbps} sha=${sha_status} runs:$times" >> "$RESULTS_DIR/l1_write_${tag}.txt"
  done
}

# ═══════════════════════════════════════════════════════════
# L2: Large File Read (500MB, 1GB) — reads files from L1
# ═══════════════════════════════════════════════════════════
bench_L2_read() {
  should_run L2 || return 0
  local sys=$1; local tag=$(tag_of $sys)
  local sizes="500"; [ "$QUICK" = "1" ] && sizes="200"
  [ "$MODE" != "quick" ] && sizes="500 1024"
  log ""; log "=== L2: Large File Sequential Read ($tag) ==="
  for sz in $sizes; do
    local label="${sz}MB"; [ "$sz" = "1024" ] && label="1GB"
    local actual_bytes=$(exec_fn $sys "wc -c < /tmp/l1-${sz}m.dat 2>/dev/null" | tr -d ' \r\n\t')
    if [ -z "$actual_bytes" ] || [ "$actual_bytes" = "0" ]; then
      log "  $label: SKIPPED (no L1 input)"
      continue
    fi
    local times=""
    for r in 1 2 3; do
      exec_fn $sys "rm -f /tmp/l2-${sz}m-r${r}.dat 2>/dev/null; true" || true
      local s=$(ts_ms)
      get_fn $sys "/bench/v2/l1-${sz}m-r1.dat" "/tmp/l2-${sz}m-r${r}.dat"
      local e=$(ts_ms)
      times="$times $((e-s))"
    done
    local avg=$(echo "$times" | awk '{s=0;n=0; for(i=2;i<=NF;i++){s+=$i;n++} if(n>0) printf "%d",s/n; else print 0}')
    local mbps="N/A"
    [ "$avg" -gt 0 ] && mbps=$(awk "BEGIN{printf \"%.1f\", $actual_bytes / 1048576 / ($avg / 1000)}")
    log "  $label: avg=${avg}ms (${mbps} MB/s) [runs:$times]"
    echo "$label avg=${avg}ms mbps=${mbps} runs:$times" >> "$RESULTS_DIR/l2_read_${tag}.txt"
  done
}

# ═══════════════════════════════════════════════════════════
# L3: Concurrent 500MB Writes (2 clients) — pipeline stress
# ═══════════════════════════════════════════════════════════
bench_L3_concurrent() {
  should_run L3 || return 0
  local sys=$1; local tag=$(tag_of $sys)
  local sz=500; [ "$QUICK" = "1" ] && sz=100
  log ""; log "=== L3: Concurrent 2×${sz}MB Write ($tag) ==="
  mkdir_fn $sys "/bench/v2/l3"
  for i in 1 2; do
    exec_fn $sys "dd if=/dev/urandom bs=1M count=$sz of=/tmp/l3-c${i}.dat 2>/dev/null"
  done
  local sha1=$(sha256_local $sys "/tmp/l3-c1.dat")
  local sha2=$(sha256_local $sys "/tmp/l3-c2.dat")
  local s=$(ts_ms)
  if [ "$sys" = "mini" ]; then
    exec_fn $sys "(hdfs put /tmp/l3-c1.dat /bench/v2/l3/c1.dat &) ; (hdfs put /tmp/l3-c2.dat /bench/v2/l3/c2.dat &) ; wait"
  else
    exec_fn $sys "(hdfs dfs -put -f /tmp/l3-c1.dat /bench/v2/l3/c1.dat &) ; (hdfs dfs -put -f /tmp/l3-c2.dat /bench/v2/l3/c2.dat &) ; wait"
  fi
  local e=$(ts_ms); local elapsed=$((e-s))
  # Verify both
  exec_fn $sys "rm -f /tmp/l3-v1.dat /tmp/l3-v2.dat 2>/dev/null; true" || true
  get_fn $sys "/bench/v2/l3/c1.dat" "/tmp/l3-v1.dat"
  get_fn $sys "/bench/v2/l3/c2.dat" "/tmp/l3-v2.dat"
  local rsha1=$(sha256_local $sys "/tmp/l3-v1.dat")
  local rsha2=$(sha256_local $sys "/tmp/l3-v2.dat")
  local pass=0
  [ "$sha1" = "$rsha1" ] && [ -n "$sha1" ] && pass=$((pass+1))
  [ "$sha2" = "$rsha2" ] && [ -n "$sha2" ] && pass=$((pass+1))
  log "  2×${sz}MB: ${elapsed}ms, integrity: $pass/2"
  echo "2x${sz}MB ${elapsed}ms sha_pass=$pass/2" >> "$RESULTS_DIR/l3_concurrent_${tag}.txt"
}

# ═══════════════════════════════════════════════════════════
# F5: NameNode Restart — read survives
# ═══════════════════════════════════════════════════════════
bench_F5_namenode_restart() {
  should_run F5 || return 0
  local sys=$1; local tag=$(tag_of $sys); local compose=$(compose_of $sys)
  local nn=$(namenode_name $sys)
  log ""; log "=== F5: NameNode Restart, Read Survives ($tag) ==="
  mkdir_fn $sys "/bench/v2/f5"
  local shas=""
  for i in 1 2 3 4 5; do
    exec_fn $sys "dd if=/dev/urandom bs=1M count=10 of=/tmp/f5-$i.dat 2>/dev/null"
    local s=$(sha256_local $sys "/tmp/f5-$i.dat")
    shas="$shas $s"
    put_fn $sys "/tmp/f5-$i.dat" "/bench/v2/f5/file-$i.dat"
  done
  log "  5 files written. Stopping NameNode..."
  local stop_s=$(ts_ms)
  docker compose -f "$compose" stop "$nn" > /dev/null 2>&1
  sleep 3
  log "  Starting NameNode..."
  docker compose -f "$compose" start "$nn" > /dev/null 2>&1
  # Wait for NN to be serving requests
  local ready=0
  for i in $(seq 1 60); do
    if exec_fn $sys "hdfs ls / 2>/dev/null >/dev/null || hdfs dfs -ls / 2>/dev/null >/dev/null"; then
      ready=1; break
    fi
    sleep 2
  done
  local ready_s=$(ts_ms)
  local restart_elapsed=$(( (ready_s - stop_s) / 1000 ))
  if [ "$ready" = "0" ]; then
    log "  NameNode did NOT become healthy within 120s"
    echo "restart_time=>120s files=UNKNOWN" >> "$RESULTS_DIR/f5_nn_restart_${tag}.txt"
    return
  fi
  # Read all 5, verify SHA
  local pass=0; local i=0
  for expected in $shas; do
    i=$((i+1))
    exec_fn $sys "rm -f /tmp/f5-v-$i.dat 2>/dev/null; true" || true
    get_fn $sys "/bench/v2/f5/file-$i.dat" "/tmp/f5-v-$i.dat"
    local actual=$(sha256_local $sys "/tmp/f5-v-$i.dat")
    [ "$expected" = "$actual" ] && [ -n "$expected" ] && pass=$((pass+1))
  done
  log "  NN restart: ${restart_elapsed}s | Files recovered with SHA match: $pass/5"
  echo "restart_time=${restart_elapsed}s sha_pass=$pass/5" >> "$RESULTS_DIR/f5_nn_restart_${tag}.txt"
}

# ═══════════════════════════════════════════════════════════
# F6: Kill-2 / Restart-2 / Content Unchanged (reviewer's exact ask)
# ═══════════════════════════════════════════════════════════
bench_F6_kill2_restart2() {
  should_run F6 || return 0
  local sys=$1; local tag=$(tag_of $sys); local compose=$(compose_of $sys)
  local w2=$(worker_name $sys 2); local w3=$(worker_name $sys 3)
  log ""; log "=== F6: Kill-2 + Restart-2 + Content Unchanged ($tag) ==="
  mkdir_fn $sys "/bench/v2/f6"
  local shas=""
  for i in 1 2 3 4 5; do
    exec_fn $sys "dd if=/dev/urandom bs=1M count=10 of=/tmp/f6-$i.dat 2>/dev/null"
    local s=$(sha256_local $sys "/tmp/f6-$i.dat")
    shas="$shas $s"
    put_fn $sys "/tmp/f6-$i.dat" "/bench/v2/f6/file-$i.dat"
  done
  log "  5 files written. Killing workers 2 and 3..."
  docker compose -f "$compose" stop "$w2" "$w3" > /dev/null 2>&1
  sleep 8
  log "  Restarting workers 2 and 3..."
  docker compose -f "$compose" start "$w2" "$w3" > /dev/null 2>&1
  sleep 20  # wait for DN re-registration + block reports
  # Read all 5, verify SHA
  local pass=0; local i=0
  for expected in $shas; do
    i=$((i+1))
    exec_fn $sys "rm -f /tmp/f6-v-$i.dat 2>/dev/null; true" || true
    get_fn $sys "/bench/v2/f6/file-$i.dat" "/tmp/f6-v-$i.dat" 2>/dev/null
    local actual=$(sha256_local $sys "/tmp/f6-v-$i.dat")
    if [ "$expected" = "$actual" ] && [ -n "$expected" ]; then
      pass=$((pass+1))
    fi
  done
  log "  After kill-2/restart-2: $pass/5 files SHA match"
  echo "sha_pass=$pass/5" >> "$RESULTS_DIR/f6_kill2_restart2_${tag}.txt"
}

# ═══════════════════════════════════════════════════════════
# F7: Pipeline Failover Mid-Write
# ═══════════════════════════════════════════════════════════
bench_F7_pipeline_failover() {
  should_run F7 || return 0
  local sys=$1; local tag=$(tag_of $sys); local compose=$(compose_of $sys)
  local w2=$(worker_name $sys 2)
  local sz=100; [ "$QUICK" = "1" ] && sz=50
  log ""; log "=== F7: Pipeline Failover Mid-Write (${sz}MB, $tag) ==="
  mkdir_fn $sys "/bench/v2/f7"
  exec_fn $sys "dd if=/dev/urandom bs=1M count=$sz of=/tmp/f7.dat 2>/dev/null"
  local orig_sha=$(sha256_local $sys "/tmp/f7.dat")
  # Start write in background, kill worker-2 mid-flight
  (
    put_fn $sys "/tmp/f7.dat" "/bench/v2/f7/f7.dat"
    echo "$?" > "$RESULTS_DIR/f7_write_rc_${tag}.txt"
  ) &
  local write_pid=$!
  sleep 2
  log "  Killing $w2 mid-write..."
  docker compose -f "$compose" stop "$w2" > /dev/null 2>&1
  wait $write_pid
  local write_rc=$(cat "$RESULTS_DIR/f7_write_rc_${tag}.txt" 2>/dev/null || echo "?")
  # Read back and verify
  exec_fn $sys "rm -f /tmp/f7-v.dat 2>/dev/null; true" || true
  get_fn $sys "/bench/v2/f7/f7.dat" "/tmp/f7-v.dat" 2>/dev/null
  local read_sha=$(sha256_local $sys "/tmp/f7-v.dat")
  local result="FAIL"
  [ "$orig_sha" = "$read_sha" ] && [ -n "$orig_sha" ] && result="PASS"
  log "  Write exit code: $write_rc | SHA match: $result"
  echo "write_rc=$write_rc sha=$result" >> "$RESULTS_DIR/f7_pipeline_${tag}.txt"
  # Restore
  docker compose -f "$compose" start "$w2" > /dev/null 2>&1
  sleep 10
}

# ═══════════════════════════════════════════════════════════
# F8: ResourceManager Restart, Job Re-submission
# ═══════════════════════════════════════════════════════════
bench_F8_rm_restart() {
  should_run F8 || return 0
  local sys=$1; local tag=$(tag_of $sys); local compose=$(compose_of $sys)
  local rm=$(rm_name $sys)
  log ""; log "=== F8: RM Restart, Job Re-submit ($tag) ==="
  # 1. Submit small job (pre-RM-kill)
  exec_fn $sys "i=0; while [ \$i -lt 5000 ]; do echo 'the quick brown fox hadoop'; i=\$((i+1)); done > /tmp/f8.txt"
  local pre_rc
  if [ "$sys" = "mini" ]; then
    exec_fn $sys "rm -rf /tmp/f8-out-pre 2>/dev/null; mapreduce --job wordcount --input /tmp/f8.txt --output /tmp/f8-out-pre --local 2>/dev/null"
    pre_rc=$?
  else
    exec_fn $sys "hdfs dfs -mkdir -p /f8 2>/dev/null; hdfs dfs -put -f /tmp/f8.txt /f8/input; hdfs dfs -rm -r /f8/out-pre 2>/dev/null; hadoop jar \$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar wordcount /f8/input /f8/out-pre 2>/dev/null"
    pre_rc=$?
  fi
  log "  Pre-RM-kill job: rc=$pre_rc"
  # 2. Kill RM
  log "  Stopping $rm..."
  local stop_s=$(ts_ms)
  docker compose -f "$compose" stop "$rm" > /dev/null 2>&1
  sleep 3
  # 3. Start RM
  log "  Starting $rm..."
  docker compose -f "$compose" start "$rm" > /dev/null 2>&1
  sleep 20  # wait for NM re-registration
  local ready_s=$(ts_ms)
  local rm_restart_elapsed=$(( (ready_s - stop_s) / 1000 ))
  # 4. Submit another job (post-RM-restart)
  local post_rc
  if [ "$sys" = "mini" ]; then
    exec_fn $sys "rm -rf /tmp/f8-out-post 2>/dev/null; mapreduce --job wordcount --input /tmp/f8.txt --output /tmp/f8-out-post --local 2>/dev/null"
    post_rc=$?
  else
    exec_fn $sys "hdfs dfs -rm -r /f8/out-post 2>/dev/null; hadoop jar \$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar wordcount /f8/input /f8/out-post 2>/dev/null"
    post_rc=$?
  fi
  log "  RM restart: ${rm_restart_elapsed}s | Pre rc=$pre_rc | Post rc=$post_rc"
  local verdict="FAIL"; [ "$pre_rc" = "0" ] && [ "$post_rc" = "0" ] && verdict="PASS"
  echo "rm_restart=${rm_restart_elapsed}s pre_rc=$pre_rc post_rc=$post_rc verdict=$verdict" >> "$RESULTS_DIR/f8_rm_restart_${tag}.txt"
}

# ═══════════════════════════════════════════════════════════
# F9: Cascade Recovery — kill, re-replicate, kill another
# ═══════════════════════════════════════════════════════════
bench_F9_cascade() {
  should_run F9 || return 0
  local sys=$1; local tag=$(tag_of $sys); local compose=$(compose_of $sys)
  local w2=$(worker_name $sys 2); local w3=$(worker_name $sys 3)
  log ""; log "=== F9: Cascade Recovery ($tag) ==="
  mkdir_fn $sys "/bench/v2/f9"
  exec_fn $sys "dd if=/dev/urandom bs=1M count=10 of=/tmp/f9.dat 2>/dev/null"
  local orig_sha=$(sha256_local $sys "/tmp/f9.dat")
  put_fn $sys "/tmp/f9.dat" "/bench/v2/f9/f9.dat"
  log "  File written. Killing $w2 (wait 30s for re-replication)..."
  docker compose -f "$compose" stop "$w2" > /dev/null 2>&1
  sleep 30
  log "  Killing $w3 (now only worker-1 should have all blocks)..."
  docker compose -f "$compose" stop "$w3" > /dev/null 2>&1
  sleep 5
  exec_fn $sys "rm -f /tmp/f9-v.dat 2>/dev/null; true" || true
  get_fn $sys "/bench/v2/f9/f9.dat" "/tmp/f9-v.dat" 2>/dev/null
  local read_sha=$(sha256_local $sys "/tmp/f9-v.dat")
  local result="FAIL"
  [ "$orig_sha" = "$read_sha" ] && [ -n "$orig_sha" ] && result="PASS"
  log "  Read after cascade: $result"
  echo "cascade=$result" >> "$RESULTS_DIR/f9_cascade_${tag}.txt"
  docker compose -f "$compose" start "$w2" "$w3" > /dev/null 2>&1
  sleep 15
}

# ═══════════════════════════════════════════════════════════
# F10: Rolling Restart Under Write Load
# ═══════════════════════════════════════════════════════════
bench_F10_rolling_restart() {
  should_run F10 || return 0
  local sys=$1; local tag=$(tag_of $sys); local compose=$(compose_of $sys)
  local duration=60; [ "$QUICK" = "1" ] && duration=30
  log ""; log "=== F10: Rolling Restart (${duration}s) ($tag) ==="
  mkdir_fn $sys "/bench/v2/f10"
  # Start write loop in background
  local writes_succeeded=0; local writes_attempted=0
  exec_fn $sys "dd if=/dev/urandom bs=1M count=1 of=/tmp/f10-src.dat 2>/dev/null"
  (
    local end=$(($(date +%s) + duration))
    local n=0
    while [ "$(date +%s)" -lt "$end" ]; do
      n=$((n+1))
      if put_fn $sys "/tmp/f10-src.dat" "/bench/v2/f10/w-$n.dat" > /dev/null 2>&1; then
        echo "ok" >> "$RESULTS_DIR/f10_writes_${tag}.log"
      else
        echo "fail" >> "$RESULTS_DIR/f10_writes_${tag}.log"
      fi
      sleep 1
    done
  ) &
  local write_pid=$!
  # Rolling restart schedule: kill w1 at 15s, restart at 22s; w2 at 30s/37s; w3 at 45s/52s
  sleep 15
  log "  Rolling: stop worker-1"
  docker compose -f "$compose" stop "$(worker_name $sys 1)" > /dev/null 2>&1
  sleep 7
  log "  Rolling: start worker-1"
  docker compose -f "$compose" start "$(worker_name $sys 1)" > /dev/null 2>&1
  sleep 8
  log "  Rolling: stop worker-2"
  docker compose -f "$compose" stop "$(worker_name $sys 2)" > /dev/null 2>&1
  sleep 7
  log "  Rolling: start worker-2"
  docker compose -f "$compose" start "$(worker_name $sys 2)" > /dev/null 2>&1
  sleep 8
  log "  Rolling: stop worker-3"
  docker compose -f "$compose" stop "$(worker_name $sys 3)" > /dev/null 2>&1
  sleep 7
  log "  Rolling: start worker-3"
  docker compose -f "$compose" start "$(worker_name $sys 3)" > /dev/null 2>&1
  wait $write_pid 2>/dev/null || true
  sleep 10  # let cluster settle
  writes_succeeded=$(grep -c "^ok$" "$RESULTS_DIR/f10_writes_${tag}.log" 2>/dev/null || echo "0")
  writes_attempted=$(wc -l < "$RESULTS_DIR/f10_writes_${tag}.log" 2>/dev/null | tr -d ' \r\n' || echo "0")
  log "  Writes: $writes_succeeded/$writes_attempted succeeded"
  echo "succeeded=$writes_succeeded attempted=$writes_attempted" >> "$RESULTS_DIR/f10_rolling_${tag}.txt"
}

# ═══════════════════════════════════════════════════════════
# D1: Sustained Mixed Workload
# ═══════════════════════════════════════════════════════════
bench_D1_sustained() {
  should_run D1 || return 0
  local sys=$1; local tag=$(tag_of $sys); local compose=$(compose_of $sys)
  local rounds=10; [ "$QUICK" = "1" ] && rounds=3
  log ""; log "=== D1: Sustained Mixed Workload ($rounds rounds, $tag) ==="
  mkdir_fn $sys "/bench/v2/d1"
  # Record idle memory
  local mem_idle=$(docker compose -f "$compose" stats --no-stream --format "{{.MemUsage}}" 2>/dev/null | head -1 | tr -d ' \r\n')
  log "  Idle memory snapshot: $mem_idle"
  echo "idle: $mem_idle" > "$RESULTS_DIR/d1_sustained_${tag}.txt"
  exec_fn $sys "dd if=/dev/urandom bs=1M count=10 of=/tmp/d1-src.dat 2>/dev/null"
  for round in $(seq 1 $rounds); do
    mkdir_fn $sys "/bench/v2/d1/r$round"
    local s=$(ts_ms)
    local ok=0; local fail=0
    for i in 1 2 3 4 5 6 7 8 9 10; do
      if put_fn $sys "/tmp/d1-src.dat" "/bench/v2/d1/r$round/f-$i.dat" > /dev/null 2>&1; then
        ok=$((ok+1))
      else
        fail=$((fail+1))
      fi
    done
    local e=$(ts_ms); local elapsed=$((e-s))
    local mbps="N/A"
    [ "$elapsed" -gt 0 ] && mbps=$(awk "BEGIN{printf \"%.1f\", 100 / ($elapsed / 1000)}")
    local mem=$(docker compose -f "$compose" stats --no-stream --format "{{.Name}}={{.MemUsage}}" 2>/dev/null | head -5 | tr '\n' '|')
    log "  Round $round: $ok/10 ok, ${elapsed}ms (${mbps}MB/s) | mem: $mem"
    echo "round$round ok=$ok fail=$fail ${elapsed}ms ${mbps}MBps mem=$mem" >> "$RESULTS_DIR/d1_sustained_${tag}.txt"
  done
}

# ═══════════════════════════════════════════════════════════
# MAIN EXECUTION
# ═══════════════════════════════════════════════════════════

run_all_v2() {
  local sys=$1
  bench_L1_write $sys
  bench_L2_read $sys
  bench_L3_concurrent $sys
  bench_F5_namenode_restart $sys
  bench_F6_kill2_restart2 $sys
  bench_F7_pipeline_failover $sys
  bench_F8_rm_restart $sys
  bench_F9_cascade $sys
  bench_F10_rolling_restart $sys
  bench_D1_sustained $sys
}

if [ "$MODE" != "hadoop-only" ]; then
  log ""; log "┌──────────────────────────────────────┐"
  log "│   mini-Hadoop v2 Benchmarks           │"
  log "└──────────────────────────────────────┘"
  start_mini
  run_all_v2 mini
  stop_mini
fi

if [ "$MODE" != "mini-only" ]; then
  log ""; log "┌──────────────────────────────────────┐"
  log "│   Apache Hadoop v2 Benchmarks         │"
  log "└──────────────────────────────────────┘"
  start_hadoop
  run_all_v2 hadoop
  stop_hadoop
fi

log ""
log "══════════════════════════════════════"
log "  v2 BENCHMARKS COMPLETE"
log "══════════════════════════════════════"
log "  Results: $RESULTS_DIR/"
log ""
ls -la "$RESULTS_DIR/" 2>/dev/null | tee -a "$RESULTS_DIR/all_results.txt"
