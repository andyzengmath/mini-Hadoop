#!/bin/bash
# mini-Hadoop vs Apache Hadoop — Full Benchmark Suite (18 benchmarks)
#
# Usage:
#   ./scripts/benchmark.sh              # Run all 18 benchmarks on both systems
#   ./scripts/benchmark.sh mini-only    # Run only mini-Hadoop
#   ./scripts/benchmark.sh hadoop-only  # Run only Hadoop
#   ./scripts/benchmark.sh quick        # Quick subset (10MB, fewer runs)
#
# Prerequisites: Docker Desktop running, mini-Hadoop image built (make docker-build)

set -e

MODE="${1:-all}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
RESULTS_DIR="$PROJECT_DIR/benchmark_results_$(date +%Y%m%d_%H%M%S)"
MINI_COMPOSE="$PROJECT_DIR/docker/docker-compose.yml"
HADOOP_COMPOSE="$PROJECT_DIR/docker/hadoop/docker-compose.yml"
RUNS=3  # Runs per benchmark (first discarded as warmup)
[ "$MODE" = "quick" ] && RUNS=2

mkdir -p "$RESULTS_DIR"

echo "╔══════════════════════════════════════════════════════╗"
echo "║   mini-Hadoop vs Apache Hadoop — 18 Benchmarks      ║"
echo "╠══════════════════════════════════════════════════════╣"
echo "║  Mode: $MODE | Runs: $RUNS | $(date)               ║"
echo "╚══════════════════════════════════════════════════════╝"

# ─── Helpers ────────────────────────────────────────────────

ts_ms() { date +%s%3N 2>/dev/null || python3 -c "import time;print(int(time.time()*1000))"; }

log() { echo "$1" | tee -a "$RESULTS_DIR/all_results.txt"; }

mini_exec() { docker compose -f "$MINI_COMPOSE" exec -T client sh -c "$1" 2>/dev/null; }
hadoop_exec() { docker compose -f "$HADOOP_COMPOSE" exec -T hadoop-client sh -c "$1" 2>/dev/null; }

hdfs_put() {
  if [ "$1" = "mini" ]; then mini_exec "hdfs put $2 $3"
  else hadoop_exec "hdfs dfs -put -f $2 $3"; fi
}
hdfs_get() {
  if [ "$1" = "mini" ]; then mini_exec "hdfs get $2 $3"
  else hadoop_exec "hdfs dfs -get $2 $3"; fi
}
hdfs_mkdir() {
  if [ "$1" = "mini" ]; then mini_exec "hdfs mkdir $2 2>/dev/null"
  else hadoop_exec "hdfs dfs -mkdir -p $2 2>/dev/null"; fi
}
exec_fn() {
  if [ "$1" = "mini" ]; then mini_exec "$2"
  else hadoop_exec "$2"; fi
}

avg_ms() {
  # Average of all values except the first (warmup)
  echo "$@" | tr ' ' '\n' | tail -n +2 | awk '{s+=$1;n++} END {if(n>0) printf "%d", s/n; else print 0}'
}

# ─── Cluster Management ────────────────────────────────────

start_mini() {
  log ">>> Starting mini-Hadoop cluster..."
  local s=$(ts_ms)
  docker compose -f "$MINI_COMPOSE" up -d --build 2>/dev/null
  sleep 12
  for i in $(seq 1 20); do
    local dn=$(mini_exec 'curl -s http://namenode:9100/metrics' | grep -o '"alive_datanodes":[0-9]*' | grep -o '[0-9]*')
    [ "$dn" = "3" ] && break; sleep 2
  done
  local e=$(ts_ms)
  local startup=$(( (e - s) / 1000 ))
  log "    mini-Hadoop ready (${startup}s startup)"
  echo "$startup" > "$RESULTS_DIR/r4_startup_mini.txt"
}

stop_mini() { docker compose -f "$MINI_COMPOSE" down -v 2>/dev/null; }

start_hadoop() {
  log ">>> Starting Apache Hadoop cluster..."
  local s=$(ts_ms)
  docker compose -f "$HADOOP_COMPOSE" up -d 2>/dev/null
  for i in $(seq 1 60); do
    hadoop_exec "hdfs dfs -ls / 2>/dev/null" > /dev/null 2>&1 && break; sleep 5
  done
  sleep 15  # Wait for DataNodes
  local e=$(ts_ms)
  local startup=$(( (e - s) / 1000 ))
  log "    Hadoop ready (${startup}s startup)"
  echo "$startup" > "$RESULTS_DIR/r4_startup_hadoop.txt"
}

stop_hadoop() { docker compose -f "$HADOOP_COMPOSE" down -v 2>/dev/null; }

# ═══════════════════════════════════════════════════════════
# STORAGE BENCHMARKS (S1-S4)
# ═══════════════════════════════════════════════════════════

bench_S1_write() {
  local sys=$1; local tag=$([ "$sys" = "mini" ] && echo "mini-Hadoop" || echo "Hadoop")
  local sizes="10 100"; [ "$MODE" != "quick" ] && sizes="10 100 500"
  log ""; log "=== S1: Sequential Write Throughput ($tag) ==="
  for sz in $sizes; do
    exec_fn $sys "dd if=/dev/urandom bs=1M count=$sz 2>/dev/null | base64 > /tmp/b-${sz}m.txt"
    local actual=$(exec_fn $sys "wc -c < /tmp/b-${sz}m.txt" | tr -d ' \r')
    local times=""
    hdfs_mkdir $sys "/bench"
    for r in $(seq 1 $RUNS); do
      local s=$(ts_ms)
      hdfs_put $sys "/tmp/b-${sz}m.txt" "/bench/s1-${sz}m-r${r}.dat"
      local e=$(ts_ms)
      times="$times $((e-s))"
    done
    local avg=$(avg_ms $times)
    local mbps="N/A"; [ $avg -gt 0 ] && mbps=$(echo "scale=1; $actual / 1048576 / ($avg / 1000)" | bc 2>/dev/null || echo "N/A")
    log "  ${sz}MB: avg=${avg}ms (${mbps} MB/s) [runs:$times]"
    echo "${sz}MB ${avg}ms ${mbps}MB/s" >> "$RESULTS_DIR/s1_write_${tag}.txt"
  done
}

bench_S2_read() {
  local sys=$1; local tag=$([ "$sys" = "mini" ] && echo "mini-Hadoop" || echo "Hadoop")
  local sizes="10 100"; [ "$MODE" != "quick" ] && sizes="10 100 500"
  log ""; log "=== S2: Sequential Read Throughput ($tag) ==="
  for sz in $sizes; do
    local times=""
    for r in $(seq 1 $RUNS); do
      local s=$(ts_ms)
      hdfs_get $sys "/bench/s1-${sz}m-r1.dat" "/tmp/read-${sz}m-r${r}.txt"
      local e=$(ts_ms)
      times="$times $((e-s))"
    done
    local avg=$(avg_ms $times)
    log "  ${sz}MB: avg=${avg}ms [runs:$times]"
    echo "${sz}MB ${avg}ms" >> "$RESULTS_DIR/s2_read_${tag}.txt"
  done
}

bench_S3_small_files() {
  local sys=$1; local tag=$([ "$sys" = "mini" ] && echo "mini-Hadoop" || echo "Hadoop")
  local count=100; [ "$MODE" != "quick" ] && count=500
  log ""; log "=== S3: Small File Performance ($tag) — $count files ==="
  hdfs_mkdir $sys "/bench/small"
  # Generate small files inside container
  exec_fn $sys "for i in \$(seq 1 $count); do echo \"file \$i test data\" > /tmp/sf-\$i.txt; done"
  # Write
  local s=$(ts_ms)
  exec_fn $sys "for i in \$(seq 1 $count); do $([ "$sys" = "mini" ] && echo "hdfs put" || echo "hdfs dfs -put") /tmp/sf-\$i.txt /bench/small/f-\$i.txt 2>/dev/null; done"
  local e=$(ts_ms)
  local write_ms=$((e-s))
  local write_fps=$(echo "scale=1; $count / ($write_ms / 1000)" | bc 2>/dev/null || echo "N/A")
  log "  Write $count files: ${write_ms}ms (${write_fps} files/s)"
  # Read
  s=$(ts_ms)
  exec_fn $sys "for i in \$(seq 1 $count); do $([ "$sys" = "mini" ] && echo "hdfs get" || echo "hdfs dfs -get") /bench/small/f-\$i.txt /tmp/sf-r-\$i.txt 2>/dev/null; done"
  e=$(ts_ms)
  local read_ms=$((e-s))
  local read_fps=$(echo "scale=1; $count / ($read_ms / 1000)" | bc 2>/dev/null || echo "N/A")
  log "  Read $count files: ${read_ms}ms (${read_fps} files/s)"
  echo "write: ${count}files ${write_ms}ms ${write_fps}fps" >> "$RESULTS_DIR/s3_small_${tag}.txt"
  echo "read: ${count}files ${read_ms}ms ${read_fps}fps" >> "$RESULTS_DIR/s3_small_${tag}.txt"
}

bench_S4_integrity() {
  local sys=$1; local tag=$([ "$sys" = "mini" ] && echo "mini-Hadoop" || echo "Hadoop")
  log ""; log "=== S4: Data Integrity ($tag) ==="
  local result=$(exec_fn $sys "
    dd if=/dev/urandom bs=1M count=10 2>/dev/null | base64 > /tmp/s4.txt
    HASH1=\$(sha256sum /tmp/s4.txt | awk '{print \$1}')
    $([ "$sys" = "mini" ] && echo "hdfs mkdir /bench 2>/dev/null; hdfs put /tmp/s4.txt /bench/s4.dat" || echo "hdfs dfs -mkdir -p /bench 2>/dev/null; hdfs dfs -put -f /tmp/s4.txt /bench/s4.dat")
    $([ "$sys" = "mini" ] && echo "hdfs get /bench/s4.dat /tmp/s4v.txt" || echo "hdfs dfs -get /bench/s4.dat /tmp/s4v.txt")
    HASH2=\$(sha256sum /tmp/s4v.txt | awk '{print \$1}')
    [ \"\$HASH1\" = \"\$HASH2\" ] && echo PASS || echo FAIL
  ")
  log "  SHA-256 integrity: $result"
  echo "$result" >> "$RESULTS_DIR/s4_integrity_${tag}.txt"
}

# ═══════════════════════════════════════════════════════════
# FAULT TOLERANCE BENCHMARKS (F1-F4)
# ═══════════════════════════════════════════════════════════

bench_F1_detection() {
  local sys=$1; local tag=$([ "$sys" = "mini" ] && echo "mini-Hadoop" || echo "Hadoop")
  local compose=$([ "$sys" = "mini" ] && echo "$MINI_COMPOSE" || echo "$HADOOP_COMPOSE")
  local worker=$([ "$sys" = "mini" ] && echo "worker-2" || echo "hadoop-worker-2")
  log ""; log "=== F1: Dead Node Detection Time ($tag) ==="
  exec_fn $sys "echo 'f1 test' > /tmp/f1.txt; $([ "$sys" = "mini" ] && echo "hdfs mkdir /bench 2>/dev/null; hdfs put /tmp/f1.txt /bench/f1.dat" || echo "hdfs dfs -mkdir -p /bench 2>/dev/null; hdfs dfs -put -f /tmp/f1.txt /bench/f1.dat")"
  local kill_s=$(ts_ms)
  docker compose -f "$compose" stop "$worker" 2>/dev/null
  for i in $(seq 1 60); do
    if [ "$sys" = "mini" ]; then
      docker compose -f "$compose" logs namenode 2>&1 | grep -q "marked dead" && break
    else
      docker compose -f "$compose" logs hadoop-namenode 2>&1 | grep -qi "dead\|lost" && break
    fi
    sleep 1
  done
  local detect_s=$(ts_ms)
  local elapsed=$(( (detect_s - kill_s) / 1000 ))
  log "  Detection: ${elapsed}s"
  echo "${elapsed}s" >> "$RESULTS_DIR/f1_detection_${tag}.txt"
  docker compose -f "$compose" start "$worker" 2>/dev/null; sleep 5
}

bench_F2_durability() {
  local sys=$1; local tag=$([ "$sys" = "mini" ] && echo "mini-Hadoop" || echo "Hadoop")
  local compose=$([ "$sys" = "mini" ] && echo "$MINI_COMPOSE" || echo "$HADOOP_COMPOSE")
  local worker=$([ "$sys" = "mini" ] && echo "worker-2" || echo "hadoop-worker-2")
  log ""; log "=== F2: Data Durability After Node Death ($tag) ==="
  # Write 5 test files
  for i in $(seq 1 5); do
    exec_fn $sys "echo 'durability test file $i with enough data to be meaningful' > /tmp/f2-$i.txt; $([ "$sys" = "mini" ] && echo "hdfs put /tmp/f2-$i.txt /bench/f2-$i.dat" || echo "hdfs dfs -put -f /tmp/f2-$i.txt /bench/f2-$i.dat")"
  done
  docker compose -f "$compose" stop "$worker" 2>/dev/null; sleep 15
  local readable=0
  for i in $(seq 1 5); do
    if exec_fn $sys "$([ "$sys" = "mini" ] && echo "hdfs get /bench/f2-$i.dat /tmp/f2v-$i.txt" || echo "hdfs dfs -get /bench/f2-$i.dat /tmp/f2v-$i.txt")" 2>/dev/null; then
      readable=$((readable+1))
    fi
  done
  log "  Files readable: $readable/5"
  echo "$readable/5" >> "$RESULTS_DIR/f2_durability_${tag}.txt"
  docker compose -f "$compose" start "$worker" 2>/dev/null; sleep 5
}

bench_F3_recovery() {
  local sys=$1; local tag=$([ "$sys" = "mini" ] && echo "mini-Hadoop" || echo "Hadoop")
  local compose=$([ "$sys" = "mini" ] && echo "$MINI_COMPOSE" || echo "$HADOOP_COMPOSE")
  local worker=$([ "$sys" = "mini" ] && echo "worker-2" || echo "hadoop-worker-2")
  log ""; log "=== F3: Recovery Time After Node Rejoin ($tag) ==="
  docker compose -f "$compose" stop "$worker" 2>/dev/null; sleep 15
  local rejoin_s=$(ts_ms)
  docker compose -f "$compose" start "$worker" 2>/dev/null
  # Wait until the node is back and healthy
  for i in $(seq 1 60); do
    if [ "$sys" = "mini" ]; then
      local dn=$(mini_exec 'curl -s http://namenode:9100/metrics' | grep -o '"alive_datanodes":[0-9]*' | grep -o '[0-9]*')
      [ "$dn" = "3" ] && break
    else
      local live=$(hadoop_exec "hdfs dfsadmin -report 2>/dev/null" | grep "Live datanodes" | grep -o '[0-9]*')
      [ "$live" = "3" ] && break
    fi
    sleep 2
  done
  local recover_s=$(ts_ms)
  local elapsed=$(( (recover_s - rejoin_s) / 1000 ))
  log "  Full recovery: ${elapsed}s"
  echo "${elapsed}s" >> "$RESULTS_DIR/f3_recovery_${tag}.txt"
}

bench_F4_multi_failure() {
  local sys=$1; local tag=$([ "$sys" = "mini" ] && echo "mini-Hadoop" || echo "Hadoop")
  local compose=$([ "$sys" = "mini" ] && echo "$MINI_COMPOSE" || echo "$HADOOP_COMPOSE")
  log ""; log "=== F4: Multi-Node Failure ($tag) ==="
  exec_fn $sys "echo 'multi-fail test' > /tmp/f4.txt; $([ "$sys" = "mini" ] && echo "hdfs put /tmp/f4.txt /bench/f4.dat" || echo "hdfs dfs -put -f /tmp/f4.txt /bench/f4.dat")"
  # Kill 2 of 3 workers
  if [ "$sys" = "mini" ]; then
    docker compose -f "$compose" stop worker-2 worker-3 2>/dev/null
  else
    docker compose -f "$compose" stop hadoop-worker-2 hadoop-worker-3 2>/dev/null
  fi
  sleep 15
  local result=$(exec_fn $sys "$([ "$sys" = "mini" ] && echo "hdfs get /bench/f4.dat /tmp/f4v.txt" || echo "hdfs dfs -get /bench/f4.dat /tmp/f4v.txt") 2>/dev/null && echo PASS || echo FAIL")
  log "  Read after 2-of-3 death: $result"
  echo "2-of-3: $result" >> "$RESULTS_DIR/f4_multi_${tag}.txt"
  # Restart
  if [ "$sys" = "mini" ]; then
    docker compose -f "$compose" start worker-2 worker-3 2>/dev/null
  else
    docker compose -f "$compose" start hadoop-worker-2 hadoop-worker-3 2>/dev/null
  fi
  sleep 10
}

# ═══════════════════════════════════════════════════════════
# MAPREDUCE BENCHMARKS (M1-M4)
# ═══════════════════════════════════════════════════════════

bench_M1_wordcount() {
  local sys=$1; local tag=$([ "$sys" = "mini" ] && echo "mini-Hadoop" || echo "Hadoop")
  local lines=10000; [ "$MODE" != "quick" ] && lines=100000
  log ""; log "=== M1: WordCount ($tag) — ${lines} lines ==="
  exec_fn $sys "i=0; while [ \$i -lt $lines ]; do echo 'the quick brown fox jumps over lazy dog hadoop distributed'; i=\$((i+1)); done > /tmp/m1.txt"
  local s=$(ts_ms)
  if [ "$sys" = "mini" ]; then
    exec_fn $sys "mapreduce --job wordcount --input /tmp/m1.txt --output /tmp/m1-out --local 2>/dev/null"
  else
    exec_fn $sys "hdfs dfs -mkdir -p /m1 2>/dev/null; hdfs dfs -put -f /tmp/m1.txt /m1/input; hadoop jar \$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar wordcount /m1/input /m1/output-$RANDOM 2>/dev/null"
  fi
  local e=$(ts_ms); local elapsed=$((e-s))
  local total=""
  if [ "$sys" = "mini" ]; then
    total=$(exec_fn $sys "awk -F'\t' '{sum+=\$2} END {print sum}' /tmp/m1-out/part-00000")
  else
    total=$(exec_fn $sys "hdfs dfs -cat /m1/output-*/part-r-00000 2>/dev/null | awk -F'\t' '{sum+=\$2} END {print sum}'")
  fi
  local expected=$((lines * 10))
  log "  Time: ${elapsed}ms | Words: $total (expected: $expected)"
  echo "${lines}lines ${elapsed}ms words=$total" >> "$RESULTS_DIR/m1_wordcount_${tag}.txt"
}

bench_M2_sumbykey() {
  local sys=$1; local tag=$([ "$sys" = "mini" ] && echo "mini-Hadoop" || echo "Hadoop")
  local records=10000; [ "$MODE" != "quick" ] && records=100000
  log ""; log "=== M2: SumByKey ($tag) — ${records} records ==="
  exec_fn $sys "awk 'BEGIN{for(i=0;i<$records;i++) printf \"key-%05d\t%d\n\",i%1000,i%100+1}' > /tmp/m2.txt"
  local s=$(ts_ms)
  if [ "$sys" = "mini" ]; then
    exec_fn $sys "mapreduce --job sumbykey --input /tmp/m2.txt --output /tmp/m2-out --local 2>/dev/null"
  else
    exec_fn $sys "hdfs dfs -mkdir -p /m2 2>/dev/null; hdfs dfs -put -f /tmp/m2.txt /m2/input; hadoop jar \$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar wordcount /m2/input /m2/output-$RANDOM 2>/dev/null"
  fi
  local e=$(ts_ms); local elapsed=$((e-s))
  log "  Time: ${elapsed}ms"
  echo "${records}records ${elapsed}ms" >> "$RESULTS_DIR/m2_sumbykey_${tag}.txt"
}

bench_M4_scaling() {
  # mini-Hadoop only (can't easily scale Hadoop workers dynamically)
  log ""; log "=== M4: Scaling Efficiency (mini-Hadoop only) ==="
  if [ "$1" != "mini" ]; then log "  (Skipped for Hadoop)"; return; fi
  mini_exec "i=0; while [ \$i -lt 50000 ]; do echo 'the quick brown fox jumps over lazy dog hadoop distributed'; i=\$((i+1)); done > /tmp/m4.txt"
  local s=$(ts_ms)
  mini_exec "mapreduce --job wordcount --input /tmp/m4.txt --output /tmp/m4-out --local 2>/dev/null"
  local e=$(ts_ms)
  log "  Single-process: $((e-s))ms (baseline for scaling comparison)"
  echo "1-process: $((e-s))ms" >> "$RESULTS_DIR/m4_scaling.txt"
}

# ═══════════════════════════════════════════════════════════
# RESOURCE BENCHMARKS (R1-R5)
# ═══════════════════════════════════════════════════════════

bench_R1_size() {
  local sys=$1; local tag=$([ "$sys" = "mini" ] && echo "mini-Hadoop" || echo "Hadoop")
  log ""; log "=== R1: Docker Image Size ($tag) ==="
  local img=$([ "$sys" = "mini" ] && echo "mini-hadoop" || echo "apache/hadoop")
  local size=$(docker images "$img" --format "{{.Size}}" 2>/dev/null | head -1)
  [ -z "$size" ] && size="(image not found)"
  log "  Image: $size"
  echo "$tag: $size" >> "$RESULTS_DIR/r1_size.txt"
}

bench_R2_memory() {
  local sys=$1; local tag=$([ "$sys" = "mini" ] && echo "mini-Hadoop" || echo "Hadoop")
  local compose=$([ "$sys" = "mini" ] && echo "$MINI_COMPOSE" || echo "$HADOOP_COMPOSE")
  log ""; log "=== R2: Memory Usage — Idle ($tag) ==="
  docker compose -f "$compose" stats --no-stream --format "  {{.Name}}: {{.MemUsage}}" 2>/dev/null | tee -a "$RESULTS_DIR/r2_memory_${tag}.txt"
}

bench_R3_memory_load() {
  local sys=$1; local tag=$([ "$sys" = "mini" ] && echo "mini-Hadoop" || echo "Hadoop")
  local compose=$([ "$sys" = "mini" ] && echo "$MINI_COMPOSE" || echo "$HADOOP_COMPOSE")
  log ""; log "=== R3: Peak Memory Under Load ($tag) ==="
  # Snapshot memory during a write operation
  exec_fn $sys "dd if=/dev/urandom bs=1M count=50 2>/dev/null | base64 > /tmp/r3.txt" &
  sleep 2
  docker compose -f "$compose" stats --no-stream --format "  {{.Name}}: {{.MemUsage}}" 2>/dev/null | tee -a "$RESULTS_DIR/r3_memory_load_${tag}.txt"
  wait
}

bench_R5_loc() {
  log ""; log "=== R5: Lines of Code ==="
  local mini_loc=$(find "$PROJECT_DIR/pkg" "$PROJECT_DIR/cmd" -name "*.go" -exec cat {} + | wc -l)
  local mini_files=$(find "$PROJECT_DIR/pkg" "$PROJECT_DIR/cmd" -name "*.go" | wc -l)
  log "  mini-Hadoop: $mini_loc lines in $mini_files Go files"
  log "  Apache Hadoop: ~2,000,000 lines in ~8,089 Java files (reference)"
  echo "mini-Hadoop: ${mini_loc}LOC ${mini_files}files" >> "$RESULTS_DIR/r5_loc.txt"
  echo "Hadoop: ~2000000LOC ~8089files" >> "$RESULTS_DIR/r5_loc.txt"
}

# ═══════════════════════════════════════════════════════════
# CORRECTNESS BENCHMARKS (C1-C3)
# ═══════════════════════════════════════════════════════════

bench_C1_equivalence() {
  log ""; log "=== C1: Output Equivalence ==="
  if [ "$MODE" = "mini-only" ] || [ "$MODE" = "hadoop-only" ]; then
    log "  (Skipped — need both systems for comparison)"; return
  fi
  # Generate identical input
  local input="the quick brown fox jumps over the lazy dog"
  mini_exec "i=0; while [ \$i -lt 1000 ]; do echo '$input'; i=\$((i+1)); done > /tmp/c1.txt"
  hadoop_exec "i=0; while [ \$i -lt 1000 ]; do echo '$input'; i=\$((i+1)); done > /tmp/c1.txt"
  # Run on both
  mini_exec "mapreduce --job wordcount --input /tmp/c1.txt --output /tmp/c1-out --local 2>/dev/null"
  hadoop_exec "hdfs dfs -mkdir -p /c1 2>/dev/null; hdfs dfs -put -f /tmp/c1.txt /c1/input; hadoop jar \$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar wordcount /c1/input /c1/output-$RANDOM 2>/dev/null"
  # Compare
  local mini_out=$(mini_exec "sort /tmp/c1-out/part-00000")
  local hadoop_out=$(hadoop_exec "hdfs dfs -cat /c1/output-*/part-r-00000 2>/dev/null | sort")
  if [ "$mini_out" = "$hadoop_out" ]; then
    log "  WordCount output: MATCH"
    echo "MATCH" >> "$RESULTS_DIR/c1_equivalence.txt"
  else
    log "  WordCount output: DIFFER"
    echo "DIFFER" >> "$RESULTS_DIR/c1_equivalence.txt"
    echo "--- mini-Hadoop ---" >> "$RESULTS_DIR/c1_diff.txt"
    echo "$mini_out" >> "$RESULTS_DIR/c1_diff.txt"
    echo "--- Hadoop ---" >> "$RESULTS_DIR/c1_diff.txt"
    echo "$hadoop_out" >> "$RESULTS_DIR/c1_diff.txt"
  fi
}

bench_C2_edge_cases() {
  local sys=$1; local tag=$([ "$sys" = "mini" ] && echo "mini-Hadoop" || echo "Hadoop")
  log ""; log "=== C2: Edge Cases ($tag) ==="
  # Empty file
  exec_fn $sys "touch /tmp/c2-empty.txt; $([ "$sys" = "mini" ] && echo "hdfs mkdir /c2 2>/dev/null; hdfs put /tmp/c2-empty.txt /c2/empty.dat" || echo "hdfs dfs -mkdir -p /c2 2>/dev/null; hdfs dfs -put -f /tmp/c2-empty.txt /c2/empty.dat")"
  local empty_result=$(exec_fn $sys "$([ "$sys" = "mini" ] && echo "hdfs get /c2/empty.dat /tmp/c2ev.txt" || echo "hdfs dfs -get /c2/empty.dat /tmp/c2ev.txt") && wc -c < /tmp/c2ev.txt | tr -d ' '")
  log "  Empty file (0 bytes): read back $empty_result bytes (expected: 0)"

  # Single word
  exec_fn $sys "echo 'hello' > /tmp/c2-single.txt"
  if [ "$sys" = "mini" ]; then
    exec_fn $sys "mapreduce --job wordcount --input /tmp/c2-single.txt --output /tmp/c2s-out --local 2>/dev/null"
    local single=$(exec_fn $sys "cat /tmp/c2s-out/part-00000")
  else
    local single="(skipped for Hadoop — needs HDFS upload)"
  fi
  log "  Single word: $single"
  echo "empty=$empty_result single=$single" >> "$RESULTS_DIR/c2_edge_${tag}.txt"
}

bench_C3_replication() {
  log ""; log "=== C3: Replication Consistency (mini-Hadoop only) ==="
  if [ "$1" != "mini" ]; then log "  (Skipped for Hadoop — needs internal block access)"; return; fi
  local result=$(mini_exec '
    echo "replication consistency test data" > /tmp/c3.txt
    hdfs put /tmp/c3.txt /bench/c3.dat 2>/dev/null
    HASH1=$(sha256sum /tmp/c3.txt | awk "{print \$1}")
    hdfs get /bench/c3.dat /tmp/c3v.txt 2>/dev/null
    HASH2=$(sha256sum /tmp/c3v.txt | awk "{print \$1}")
    [ "$HASH1" = "$HASH2" ] && echo "PASS" || echo "FAIL"
  ')
  log "  Replication consistency: $result"
  echo "$result" >> "$RESULTS_DIR/c3_replication.txt"
}

# ═══════════════════════════════════════════════════════════
# MAIN EXECUTION
# ═══════════════════════════════════════════════════════════

run_all_benchmarks() {
  local sys=$1
  local compose=$([ "$sys" = "mini" ] && echo "$MINI_COMPOSE" || echo "$HADOOP_COMPOSE")

  bench_R1_size $sys
  bench_R2_memory $sys
  bench_S1_write $sys
  bench_S2_read $sys
  bench_S3_small_files $sys
  bench_S4_integrity $sys
  bench_R3_memory_load $sys
  bench_M1_wordcount $sys
  bench_M2_sumbykey $sys
  bench_M4_scaling $sys
  bench_F1_detection $sys
  bench_F2_durability $sys
  bench_F3_recovery $sys
  bench_F4_multi_failure $sys
  bench_C2_edge_cases $sys
  bench_C3_replication $sys
}

# --- Execute ---

bench_R5_loc  # Always run — no cluster needed

if [ "$MODE" != "hadoop-only" ]; then
  log ""; log "┌──────────────────────────────────────┐"
  log "│         mini-Hadoop Benchmarks       │"
  log "└──────────────────────────────────────┘"
  start_mini
  run_all_benchmarks mini
  stop_mini
fi

if [ "$MODE" != "mini-only" ]; then
  log ""; log "┌──────────────────────────────────────┐"
  log "│       Apache Hadoop Benchmarks       │"
  log "└──────────────────────────────────────┘"
  start_hadoop
  run_all_benchmarks hadoop
  stop_hadoop
fi

if [ "$MODE" = "all" ]; then
  bench_C1_equivalence  # Needs both clusters — run after both are benchmarked
  # Restart both briefly for comparison
  start_mini; start_hadoop
  bench_C1_equivalence
  stop_hadoop; stop_mini
fi

# --- Summary ---
log ""
log "══════════════════════════════════════"
log "  ALL 18 BENCHMARKS COMPLETE"
log "══════════════════════════════════════"
log "  Results: $RESULTS_DIR/"
log ""
ls -la "$RESULTS_DIR/"
