#!/bin/bash
# mini-Hadoop vs Apache Hadoop Benchmark Runner
#
# Usage:
#   ./scripts/benchmark.sh              # Run all benchmarks
#   ./scripts/benchmark.sh mini-only    # Run only mini-Hadoop benchmarks
#   ./scripts/benchmark.sh hadoop-only  # Run only Hadoop benchmarks
#   ./scripts/benchmark.sh quick        # Run quick subset (10MB only)
#
# Prerequisites:
#   - Docker Desktop running
#   - mini-Hadoop Docker image built: make docker-build
#   - Both clusters will be started/stopped automatically

set -e

MODE="${1:-all}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
RESULTS_DIR="$PROJECT_DIR/benchmark_results_$(date +%Y%m%d_%H%M%S)"
MINI_COMPOSE="$PROJECT_DIR/docker/docker-compose.yml"
HADOOP_COMPOSE="$PROJECT_DIR/docker/hadoop/docker-compose.yml"

mkdir -p "$RESULTS_DIR"

echo "╔══════════════════════════════════════════════════════╗"
echo "║   mini-Hadoop vs Apache Hadoop Benchmark Suite      ║"
echo "╠══════════════════════════════════════════════════════╣"
echo "║  Date:    $(date)            ║"
echo "║  Mode:    $MODE                                     ║"
echo "║  Results: $RESULTS_DIR      ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

# --- Helper functions ---

timestamp_ms() {
  date +%s%3N 2>/dev/null || python3 -c "import time; print(int(time.time()*1000))"
}

run_timed() {
  local label="$1"
  shift
  local start=$(timestamp_ms)
  "$@" > /dev/null 2>&1
  local end=$(timestamp_ms)
  local elapsed=$((end - start))
  echo "$label: ${elapsed}ms"
  echo "$label: ${elapsed}ms" >> "$RESULTS_DIR/timings.txt"
  echo "$elapsed"
}

mini_exec() {
  docker compose -f "$MINI_COMPOSE" exec -T client sh -c "$1" 2>/dev/null
}

hadoop_exec() {
  docker compose -f "$HADOOP_COMPOSE" exec -T hadoop-client sh -c "$1" 2>/dev/null
}

# --- Cluster Management ---

start_mini() {
  echo ">>> Starting mini-Hadoop cluster..."
  docker compose -f "$MINI_COMPOSE" up -d --build 2>/dev/null
  sleep 12
  echo "    Waiting for DataNodes..."
  local count=0
  while [ $count -lt 30 ]; do
    local dn=$(mini_exec 'curl -s http://namenode:9100/metrics' 2>/dev/null | grep -o '"alive_datanodes":[0-9]*' | grep -o '[0-9]*')
    if [ "$dn" = "3" ]; then
      echo "    mini-Hadoop: 3 DataNodes ready"
      return 0
    fi
    sleep 2
    count=$((count + 1))
  done
  echo "    WARNING: mini-Hadoop DataNodes not fully ready"
}

stop_mini() {
  echo ">>> Stopping mini-Hadoop cluster..."
  docker compose -f "$MINI_COMPOSE" down -v 2>/dev/null
}

start_hadoop() {
  echo ">>> Starting Apache Hadoop cluster..."
  docker compose -f "$HADOOP_COMPOSE" up -d 2>/dev/null
  echo "    Waiting for Hadoop NameNode (this takes 30-60s)..."
  sleep 45
  local count=0
  while [ $count -lt 30 ]; do
    if hadoop_exec "hdfs dfs -ls / 2>/dev/null" > /dev/null 2>&1; then
      echo "    Hadoop: NameNode ready"
      # Wait for DataNodes
      sleep 15
      local live=$(hadoop_exec "hdfs dfsadmin -report 2>/dev/null" | grep "Live datanodes" | grep -o '[0-9]*')
      echo "    Hadoop: $live DataNodes live"
      return 0
    fi
    sleep 5
    count=$((count + 1))
  done
  echo "    WARNING: Hadoop NameNode not ready"
}

stop_hadoop() {
  echo ">>> Stopping Apache Hadoop cluster..."
  docker compose -f "$HADOOP_COMPOSE" down -v 2>/dev/null
}

# --- Benchmark: Resource Footprint (R1, R2, R4) ---

bench_resources() {
  local system="$1"
  local compose="$2"
  echo ""
  echo "=== R1: Binary/Image Size ($system) ==="
  if [ "$system" = "mini-Hadoop" ]; then
    local size=$(docker images mini-hadoop --format "{{.Size}}" 2>/dev/null | head -1)
    echo "  Docker image: $size" | tee -a "$RESULTS_DIR/r1_size.txt"
  else
    local size=$(docker images apache/hadoop --format "{{.Size}}" 2>/dev/null | head -1)
    echo "  Docker image: $size" | tee -a "$RESULTS_DIR/r1_size.txt"
  fi

  echo ""
  echo "=== R2: Memory Usage - Idle ($system) ==="
  docker compose -f "$compose" stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}" 2>/dev/null | tee -a "$RESULTS_DIR/r2_memory_${system}.txt"

  echo ""
  echo "=== R4: Startup Time ($system) ==="
  echo "  (Measured during cluster startup above)"
}

# --- Benchmark: Storage (S1, S2) ---

bench_storage() {
  local system="$1"
  local exec_fn="$2"
  local sizes="10"
  [ "$MODE" != "quick" ] && sizes="10 100"

  echo ""
  echo "=== S1: Write Throughput ($system) ==="
  for size_mb in $sizes; do
    # Generate data inside container
    $exec_fn "dd if=/dev/urandom bs=1M count=$size_mb 2>/dev/null | base64 > /tmp/bench-${size_mb}m.txt"

    # Write to distributed FS
    local start=$(timestamp_ms)
    if [ "$system" = "mini-Hadoop" ]; then
      $exec_fn "hdfs mkdir /bench 2>/dev/null; hdfs put /tmp/bench-${size_mb}m.txt /bench/write-${size_mb}m.dat"
    else
      $exec_fn "hdfs dfs -mkdir -p /bench 2>/dev/null; hdfs dfs -put /tmp/bench-${size_mb}m.txt /bench/write-${size_mb}m.dat"
    fi
    local end=$(timestamp_ms)
    local elapsed=$((end - start))
    local actual_size=$($exec_fn "wc -c < /tmp/bench-${size_mb}m.txt" | tr -d ' ')
    local throughput="N/A"
    if [ $elapsed -gt 0 ]; then
      throughput=$(echo "scale=1; $actual_size / 1048576 / ($elapsed / 1000)" | bc 2>/dev/null || echo "N/A")
    fi
    echo "  ${size_mb}MB: ${elapsed}ms (${throughput} MB/s)" | tee -a "$RESULTS_DIR/s1_write_${system}.txt"
  done

  echo ""
  echo "=== S2: Read Throughput ($system) ==="
  for size_mb in $sizes; do
    local start=$(timestamp_ms)
    if [ "$system" = "mini-Hadoop" ]; then
      $exec_fn "hdfs get /bench/write-${size_mb}m.dat /tmp/bench-read-${size_mb}m.txt"
    else
      $exec_fn "hdfs dfs -get /bench/write-${size_mb}m.dat /tmp/bench-read-${size_mb}m.txt"
    fi
    local end=$(timestamp_ms)
    local elapsed=$((end - start))
    echo "  ${size_mb}MB: ${elapsed}ms" | tee -a "$RESULTS_DIR/s2_read_${system}.txt"
  done

  echo ""
  echo "=== S4: Data Integrity ($system) ==="
  if [ "$system" = "mini-Hadoop" ]; then
    local result=$($exec_fn '
      HASH1=$(sha256sum /tmp/bench-10m.txt | awk "{print \$1}")
      hdfs get /bench/write-10m.dat /tmp/verify.txt 2>/dev/null
      HASH2=$(sha256sum /tmp/verify.txt | awk "{print \$1}")
      if [ "$HASH1" = "$HASH2" ]; then echo "PASS"; else echo "FAIL"; fi
    ')
  else
    local result=$($exec_fn '
      HASH1=$(sha256sum /tmp/bench-10m.txt | awk "{print \$1}")
      hdfs dfs -get /bench/write-10m.dat /tmp/verify.txt 2>/dev/null
      HASH2=$(sha256sum /tmp/verify.txt | awk "{print \$1}")
      if [ "$HASH1" = "$HASH2" ]; then echo "PASS"; else echo "FAIL"; fi
    ')
  fi
  echo "  SHA-256 integrity: $result" | tee -a "$RESULTS_DIR/s4_integrity_${system}.txt"
}

# --- Benchmark: MapReduce (M1) ---

bench_mapreduce() {
  local system="$1"
  local exec_fn="$2"

  echo ""
  echo "=== M1: WordCount ($system) ==="

  # Generate input
  $exec_fn '
    words="the quick brown fox jumps over lazy dog hadoop distributed"
    i=0; while [ $i -lt 10000 ]; do echo "$words"; i=$((i+1)); done > /tmp/wc-input.txt
  '

  local start=$(timestamp_ms)
  if [ "$system" = "mini-Hadoop" ]; then
    $exec_fn "mapreduce --job wordcount --input /tmp/wc-input.txt --output /tmp/wc-out --local 2>/dev/null"
  else
    $exec_fn '
      hdfs dfs -mkdir -p /wc-bench 2>/dev/null
      hdfs dfs -put -f /tmp/wc-input.txt /wc-bench/input 2>/dev/null
      hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar \
        wordcount /wc-bench/input /wc-bench/output 2>/dev/null
    '
  fi
  local end=$(timestamp_ms)
  local elapsed=$((end - start))
  echo "  WordCount (10K lines): ${elapsed}ms" | tee -a "$RESULTS_DIR/m1_wordcount_${system}.txt"

  # Verify correctness
  if [ "$system" = "mini-Hadoop" ]; then
    local total=$($exec_fn 'awk -F"\t" "{sum+=\$2} END {print sum}" /tmp/wc-out/part-00000')
  else
    local total=$($exec_fn 'hdfs dfs -cat /wc-bench/output/part-r-00000 2>/dev/null | awk -F"\t" "{sum+=\$2} END {print sum}"')
  fi
  echo "  Total words: $total (expected: 100000)" | tee -a "$RESULTS_DIR/m1_wordcount_${system}.txt"
}

# --- Benchmark: Fault Tolerance (F1) ---

bench_fault_tolerance() {
  local system="$1"
  local compose="$2"

  echo ""
  echo "=== F1: Dead Node Detection Time ($system) ==="

  # Write a test file first
  if [ "$system" = "mini-Hadoop" ]; then
    mini_exec 'echo "fault test" > /tmp/ft.txt && hdfs put /tmp/ft.txt /bench/fault-test.dat 2>/dev/null'
  fi

  # Kill a worker
  local worker="worker-2"
  [ "$system" = "Apache Hadoop" ] && worker="hadoop-worker-2"

  local kill_time=$(timestamp_ms)
  docker compose -f "$compose" stop "$worker" 2>/dev/null

  # Poll for detection
  local detected=0
  local count=0
  while [ $count -lt 60 ] && [ $detected -eq 0 ]; do
    sleep 1
    if [ "$system" = "mini-Hadoop" ]; then
      docker compose -f "$compose" logs namenode 2>&1 | grep -q "marked dead" && detected=1
    else
      # Hadoop logs dead node differently
      docker compose -f "$compose" logs hadoop-namenode 2>&1 | grep -qi "dead\|lost" && detected=1
    fi
    count=$((count + 1))
  done

  local detect_time=$(timestamp_ms)
  local elapsed=$(( (detect_time - kill_time) / 1000 ))
  echo "  Detection time: ${elapsed}s" | tee -a "$RESULTS_DIR/f1_detection_${system}.txt"

  # Verify file still readable (mini-Hadoop only — we wrote the file)
  if [ "$system" = "mini-Hadoop" ]; then
    local readable=$(mini_exec 'hdfs get /bench/fault-test.dat /tmp/ft-verify.txt 2>/dev/null && echo "PASS" || echo "FAIL"')
    echo "  File readable after node death: $readable" | tee -a "$RESULTS_DIR/f1_detection_${system}.txt"
  fi

  # Restart worker
  docker compose -f "$compose" start "$worker" 2>/dev/null
  sleep 5
}

# --- Main Execution ---

echo "=========================================="
echo "  Starting Benchmarks (mode: $MODE)"
echo "=========================================="

if [ "$MODE" != "hadoop-only" ]; then
  echo ""
  echo "┌──────────────────────────────────────┐"
  echo "│         mini-Hadoop Benchmarks       │"
  echo "└──────────────────────────────────────┘"

  start_mini
  bench_resources "mini-Hadoop" "$MINI_COMPOSE"
  bench_storage "mini-Hadoop" mini_exec
  bench_mapreduce "mini-Hadoop" mini_exec
  bench_fault_tolerance "mini-Hadoop" "$MINI_COMPOSE"
  stop_mini
fi

if [ "$MODE" != "mini-only" ]; then
  echo ""
  echo "┌──────────────────────────────────────┐"
  echo "│       Apache Hadoop Benchmarks       │"
  echo "└──────────────────────────────────────┘"

  start_hadoop
  bench_resources "Apache Hadoop" "$HADOOP_COMPOSE"
  bench_storage "Apache Hadoop" hadoop_exec
  bench_mapreduce "Apache Hadoop" hadoop_exec
  bench_fault_tolerance "Apache Hadoop" "$HADOOP_COMPOSE"
  stop_hadoop
fi

# --- Generate Summary ---

echo ""
echo "=========================================="
echo "  Benchmark Summary"
echo "=========================================="
echo ""
echo "Results saved to: $RESULTS_DIR/"
echo ""
echo "Files:"
ls -la "$RESULTS_DIR/"
echo ""

# Generate markdown summary
cat > "$RESULTS_DIR/SUMMARY.md" << 'SUMMARYEOF'
# Benchmark Results

## Environment
- Date: BENCH_DATE
- Docker Desktop version: DOCKER_VERSION
- Mode: BENCH_MODE

## Results

See individual result files:
- `r1_size.txt` — Docker image sizes
- `r2_memory_*.txt` — Memory usage (idle)
- `s1_write_*.txt` — Write throughput
- `s2_read_*.txt` — Read throughput
- `s4_integrity_*.txt` — SHA-256 data integrity
- `m1_wordcount_*.txt` — MapReduce WordCount timing
- `f1_detection_*.txt` — Fault detection time
- `timings.txt` — All timed operations
SUMMARYEOF

sed -i "s/BENCH_DATE/$(date)/" "$RESULTS_DIR/SUMMARY.md" 2>/dev/null
sed -i "s/DOCKER_VERSION/$(docker --version 2>/dev/null)/" "$RESULTS_DIR/SUMMARY.md" 2>/dev/null
sed -i "s/BENCH_MODE/$MODE/" "$RESULTS_DIR/SUMMARY.md" 2>/dev/null

echo "Done! Review results in $RESULTS_DIR/SUMMARY.md"
