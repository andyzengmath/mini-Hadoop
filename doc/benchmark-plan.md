# mini-Hadoop vs Apache Hadoop: Comprehensive Benchmark Plan

**Date:** 2026-04-09
**Purpose:** Quantitatively compare mini-Hadoop against Apache Hadoop across performance, correctness, fault tolerance, and operational metrics. Establish where mini-Hadoop achieves parity, where it trades off, and where it exceeds.

---

## 1. Benchmark Philosophy

### What We're Measuring
mini-Hadoop is NOT a drop-in replacement for Hadoop. It's a **minimal reimplementation of core primitives**. The benchmark should answer:

1. **Does it produce correct results?** (same output as Hadoop for identical input)
2. **How does throughput compare?** (MB/s for storage, jobs/min for processing)
3. **How does fault tolerance compare?** (recovery time, data durability)
4. **What's the resource footprint?** (memory, CPU, disk, binary size)
5. **Where does it break?** (scale limits, edge cases)

### What We're NOT Measuring
- Enterprise features (Kerberos, multi-tenant fairness, federation)
- Ecosystem compatibility (Hive, HBase, Spark on YARN)
- Production readiness (logging, monitoring depth, admin tooling)

---

## 2. Test Environment

### Option A: Local Machine (Quick Comparison)

```
Hardware: Any machine with 16GB+ RAM, 4+ cores
mini-Hadoop: 3-node Docker Compose cluster
Hadoop:      Pseudo-distributed mode (single machine, all daemons)
```

### Option B: Multi-Machine (Realistic Comparison)

```
Hardware: 4 machines (1 master + 3 workers)
  Master: 4 cores, 8GB RAM
  Worker: 4 cores, 8GB RAM, 100GB disk
  Network: 1Gbps Ethernet

mini-Hadoop: Native binaries, 3 workers
Hadoop:      Apache Hadoop 3.3.6, 3 DataNodes + 3 NodeManagers
```

### Setup: Apache Hadoop (Pseudo-Distributed)

```bash
# Download and extract
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar xzf hadoop-3.3.6.tar.gz
export HADOOP_HOME=$(pwd)/hadoop-3.3.6
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Configure (core-site.xml, hdfs-site.xml, mapred-site.xml, yarn-site.xml)
# Set replication=3 for both systems
# Set block size=128MB for both systems

# Start services
start-dfs.sh
start-yarn.sh
```

### Setup: mini-Hadoop

```bash
# Docker Compose (local)
make docker-build && make docker-up

# OR native binaries (multi-machine)
./bin/namenode --port 9000 &
./bin/resourcemanager --port 9010 &
# ... workers on separate machines
```

---

## 3. Benchmark Categories

### 3.1 HDFS Storage Benchmarks

#### Benchmark S1: Sequential Write Throughput

**What:** Write files of increasing size and measure MB/s.

```
Sizes: 10MB, 100MB, 500MB, 1GB, 5GB
Replication: 3
Measurement: wall-clock time from first byte to CompleteFile
Metric: throughput = file_size / elapsed_time (MB/s)
Runs: 5 per size (discard first, average remaining 4)
```

| Size | mini-Hadoop (MB/s) | Hadoop (MB/s) | Ratio |
|------|-------------------|---------------|-------|
| 10MB | | | |
| 100MB | | | |
| 500MB | | | |
| 1GB | | | |
| 5GB | | | |

**Commands:**
```bash
# mini-Hadoop
time hdfs put /tmp/test-1gb.dat /bench/write-1gb.dat

# Apache Hadoop
time hadoop fs -put /tmp/test-1gb.dat /bench/write-1gb.dat
```

#### Benchmark S2: Sequential Read Throughput

**What:** Read files of increasing size and measure MB/s.

```
Sizes: Same as S1 (read files written in S1)
Metric: throughput = file_size / elapsed_time (MB/s)
```

**Commands:**
```bash
# mini-Hadoop
time hdfs get /bench/write-1gb.dat /dev/null

# Apache Hadoop
time hadoop fs -get /bench/write-1gb.dat /dev/null
```

#### Benchmark S3: Small File Performance

**What:** Write/read many small files to measure metadata overhead.

```
File count: 100, 1000, 10000
File size: 1KB each
Metric: files/second for write, files/second for read
```

This tests NameNode metadata handling speed. Hadoop is known to struggle with many small files — mini-Hadoop's in-memory namespace may perform better.

#### Benchmark S4: Data Integrity Under Load

**What:** Concurrent writes from multiple clients, verify all SHA-256 checksums.

```
Clients: 1, 2, 4, 8 (concurrent)
File size: 100MB each
Metric: all checksums correct (pass/fail), aggregate throughput
```

---

### 3.2 Fault Tolerance Benchmarks

#### Benchmark F1: Dead Node Detection Time

**What:** Kill a DataNode and measure how long until the NameNode marks it dead.

```
Procedure:
  1. Write a test file (100MB, replication=3)
  2. Note block locations
  3. Kill one DataNode (docker stop / kill -9)
  4. Measure time until NameNode logs "marked dead"
  5. Measure time until re-replication command is issued

Metric: detection_time (seconds), replication_start_time (seconds)
```

| Metric | mini-Hadoop | Hadoop | Notes |
|--------|------------|--------|-------|
| Heartbeat interval | 3s | 3s (default) | Same |
| Dead node timeout | 10s | 10m 30s (default) | mini-Hadoop is much faster |
| Detection time | ~11s (measured) | ~10m 30s (default) | |
| Re-replication start | ~11s | ~10m 30s | |

**Note:** Hadoop's default `dfs.namenode.heartbeat.recheck-interval` is 5 minutes, making dead node detection very slow by default. mini-Hadoop uses a 10-second timeout for faster recovery. For fair comparison, configure Hadoop with the same 10-second timeout.

#### Benchmark F2: Data Durability After Node Death

**What:** Kill a DataNode, verify all files are still readable.

```
Procedure:
  1. Write 10 files (10MB-100MB each)
  2. Kill one DataNode
  3. Read all 10 files, verify SHA-256 checksums
  4. Count: how many files readable? How many blocks under-replicated?

Metric: files_readable / total_files, under_replicated_blocks
```

#### Benchmark F3: Recovery Time After Node Rejoin

**What:** Kill a DataNode, wait for re-replication, restart the DataNode, measure time until cluster is fully healthy.

```
Procedure:
  1. Write files, kill DataNode, wait for re-replication
  2. Restart the DataNode
  3. Measure time until all blocks return to replication=3

Metric: full_recovery_time (seconds)
```

#### Benchmark F4: Multi-Node Failure (Stress Test)

**What:** Kill 2 of 3 DataNodes simultaneously. With replication=3, the data should still have 1 surviving replica.

```
Procedure:
  1. Write test file with replication=3
  2. Kill 2 DataNodes
  3. Attempt to read file (should succeed from surviving node)
  4. Kill all 3 → expect read failure (data loss)

Metric: pass/fail at each failure count
```

| Nodes killed | Replication=3 expected | mini-Hadoop | Hadoop |
|-------------|----------------------|------------|--------|
| 0 | Read OK | | |
| 1 | Read OK | | |
| 2 | Read OK (degraded) | | |
| 3 | Read FAIL (data loss) | | |

---

### 3.3 MapReduce Processing Benchmarks

#### Benchmark M1: WordCount (Standard Benchmark)

**What:** The canonical Hadoop benchmark — count words in a text file.

```
Input sizes: 100MB, 500MB, 1GB
Reducers: 1, 2, 4
Metric: wall-clock time, correctness (output matches)
```

**Commands:**
```bash
# Generate input
python3 -c "
import random
words = 'the quick brown fox jumps over lazy dog hadoop spark yarn hdfs block'.split()
for _ in range(10_000_000):
    print(' '.join(random.choices(words, k=10)))
" > /tmp/wordcount-input.txt

# mini-Hadoop
time mapreduce --job wordcount --input /tmp/wordcount-input.txt --output /tmp/mini-wc --local

# Apache Hadoop
hadoop fs -put /tmp/wordcount-input.txt /bench/wc-input
time hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar \
  wordcount /bench/wc-input /bench/wc-output
```

#### Benchmark M2: SumByKey (Aggregation)

**What:** Sum numeric values by key — tests shuffle performance.

```
Input: 10M records, 100K unique keys, random values 1-1000
Metric: wall-clock time, output correctness
```

#### Benchmark M3: Sort (I/O Intensive)

**What:** Sort a large file by key — the most I/O-intensive MapReduce job.

```
Input sizes: 100MB, 1GB
Metric: wall-clock time, output sorted correctly
```

This tests the full sort/spill/merge/shuffle pipeline.

#### Benchmark M4: Scaling Efficiency

**What:** Run the same job on 1, 2, and 3 workers. Measure how throughput scales.

```
Job: WordCount on 500MB
Workers: 1, 2, 3
Metric: throughput_N / (N × throughput_1) — ideal = 1.0

Expected:
  1 worker:  baseline throughput
  2 workers: ~1.7-1.9x (overhead from shuffle)
  3 workers: ~2.1-2.7x (30% threshold from AC-7)
```

| Workers | mini-Hadoop (s) | Hadoop (s) | mini speedup | Hadoop speedup |
|---------|----------------|------------|--------------|----------------|
| 1 | | | 1.0x | 1.0x |
| 2 | | | | |
| 3 | | | | |

---

### 3.4 Resource Footprint Benchmarks

#### Benchmark R1: Binary Size

| Component | mini-Hadoop | Hadoop | Ratio |
|-----------|------------|--------|-------|
| NameNode binary | `ls -lh bin/namenode` | `du -sh $HADOOP_HOME` | |
| DataNode binary | `ls -lh bin/datanode` | (included in above) | |
| All binaries | `du -sh bin/` | `du -sh $HADOOP_HOME` | |
| Docker image | `docker images mini-hadoop` | `docker images hadoop` | |

Expected: mini-Hadoop binaries ~50MB total vs Hadoop ~500MB+

#### Benchmark R2: Memory Usage (Idle Cluster)

**What:** Measure RSS of each process with no active jobs.

```bash
# mini-Hadoop (inside Docker)
docker stats --no-stream

# Hadoop
ps aux | grep -E "(NameNode|DataNode|ResourceManager|NodeManager)" | awk '{print $6/1024 "MB", $11}'
```

| Process | mini-Hadoop (MB) | Hadoop (MB) | Ratio |
|---------|-----------------|-------------|-------|
| NameNode | | | |
| DataNode | | | |
| ResourceManager | | | |
| NodeManager | | | |
| **Total** | | | |

Expected: mini-Hadoop ~50-100MB total vs Hadoop ~2-4GB (JVM heap)

#### Benchmark R3: Memory Under Load

**What:** Measure peak memory during a 1GB WordCount job.

```bash
# Monitor during job
while true; do docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}" ; sleep 2; done
```

#### Benchmark R4: Startup Time

**What:** Measure time from process start to "ready to serve" (first heartbeat processed).

```bash
# mini-Hadoop
time (./bin/namenode &; sleep 0.5; ./bin/datanode --id w1 --port 9001 &; wait)

# Hadoop
time start-dfs.sh
```

Expected: mini-Hadoop < 2 seconds vs Hadoop 15-30 seconds

#### Benchmark R5: Lines of Code

| Metric | mini-Hadoop | Hadoop |
|--------|------------|--------|
| Total LOC | ~24,000 | ~2,000,000 |
| Source files | ~65 | ~8,089 |
| Dependencies | 3 (grpc, protobuf, uuid) | 200+ JARs |
| Build time | ~5s | ~5-10 min |

---

### 3.5 Correctness Benchmarks

#### Benchmark C1: Output Equivalence

**What:** Run identical WordCount on both systems with the same input. Verify output is byte-for-byte identical.

```bash
# Generate deterministic input
python3 -c "
for i in range(100000):
    print(f'line {i} the quick brown fox jumps over the lazy dog')
" > /tmp/identical-input.txt

# Run on both systems
# mini-Hadoop
mapreduce --job wordcount --input /tmp/identical-input.txt --output /tmp/mini-out --local
sort /tmp/mini-out/part-00000 > /tmp/mini-sorted.txt

# Hadoop
hadoop jar ... wordcount /bench/input /bench/output
hadoop fs -getmerge /bench/output /tmp/hadoop-out.txt
sort /tmp/hadoop-out.txt > /tmp/hadoop-sorted.txt

# Compare
diff /tmp/mini-sorted.txt /tmp/hadoop-sorted.txt
```

**Pass criterion:** `diff` produces no output (outputs are identical).

#### Benchmark C2: Edge Cases

Test both systems with edge-case inputs:

| Input | Expected behavior |
|-------|------------------|
| Empty file (0 bytes) | Job completes, empty output |
| Single character file | 1 word, count=1 |
| 1M identical lines | 1 unique word, count=1M |
| Unicode text (CJK, emoji) | Characters handled or skipped consistently |
| Very long lines (>1MB) | No crash, line processed or truncated |
| Binary file (random bytes) | No crash, garbage words counted |

#### Benchmark C3: Replication Consistency

**What:** Write a file, verify all 3 replicas are byte-identical.

```bash
# mini-Hadoop: read from each DataNode explicitly
for node in worker-1 worker-2 worker-3; do
  # Read block directly from each node, compute SHA-256
done

# All 3 SHA-256 hashes must match
```

---

## 4. Benchmark Execution Script

```bash
#!/bin/bash
# benchmark.sh — Run all benchmarks and collect results

RESULTS_DIR="benchmark_results_$(date +%Y%m%d_%H%M%S)"
mkdir -p $RESULTS_DIR

echo "=== mini-Hadoop vs Hadoop Benchmark ==="
echo "Date: $(date)"
echo "Results: $RESULTS_DIR"

# --- S1: Write Throughput ---
echo "=== S1: Sequential Write Throughput ==="
for SIZE_MB in 10 100 500; do
  dd if=/dev/urandom bs=1M count=$SIZE_MB of=/tmp/bench-${SIZE_MB}m.dat 2>/dev/null

  echo "mini-Hadoop: ${SIZE_MB}MB write"
  START=$(date +%s%N)
  hdfs put /tmp/bench-${SIZE_MB}m.dat /bench/s1-${SIZE_MB}m.dat
  END=$(date +%s%N)
  ELAPSED_MS=$(( (END - START) / 1000000 ))
  THROUGHPUT=$(echo "scale=2; $SIZE_MB / ($ELAPSED_MS / 1000)" | bc)
  echo "  ${ELAPSED_MS}ms, ${THROUGHPUT} MB/s" | tee -a $RESULTS_DIR/s1-write.txt
done

# --- S2: Read Throughput ---
echo "=== S2: Sequential Read Throughput ==="
for SIZE_MB in 10 100 500; do
  START=$(date +%s%N)
  hdfs get /bench/s1-${SIZE_MB}m.dat /dev/null
  END=$(date +%s%N)
  ELAPSED_MS=$(( (END - START) / 1000000 ))
  THROUGHPUT=$(echo "scale=2; $SIZE_MB / ($ELAPSED_MS / 1000)" | bc)
  echo "  ${SIZE_MB}MB read: ${ELAPSED_MS}ms, ${THROUGHPUT} MB/s" | tee -a $RESULTS_DIR/s2-read.txt
done

# --- F1: Dead Node Detection ---
echo "=== F1: Dead Node Detection ==="
echo "test data" > /tmp/f1-test.txt
hdfs put /tmp/f1-test.txt /bench/f1-test.dat
echo "Killing worker-2..."
KILL_TIME=$(date +%s)
docker compose stop worker-2
# Poll NameNode logs for "marked dead"
while ! docker compose logs namenode 2>&1 | grep -q "marked dead.*worker-2"; do
  sleep 1
done
DETECT_TIME=$(date +%s)
DETECTION_SECONDS=$((DETECT_TIME - KILL_TIME))
echo "  Detection time: ${DETECTION_SECONDS}s" | tee -a $RESULTS_DIR/f1-detection.txt
docker compose start worker-2

# --- M1: WordCount ---
echo "=== M1: WordCount ==="
# Generate input (deterministic)
python3 -c "
import random; random.seed(42)
words = 'the quick brown fox jumps over lazy dog hadoop distributed system block'.split()
for _ in range(1_000_000):
    print(' '.join(random.choices(words, k=10)))
" > /tmp/m1-input.txt
SIZE_MB=$(du -m /tmp/m1-input.txt | awk '{print $1}')
echo "Input: ${SIZE_MB}MB"

START=$(date +%s%N)
mapreduce --job wordcount --input /tmp/m1-input.txt --output /tmp/m1-output --local
END=$(date +%s%N)
ELAPSED_MS=$(( (END - START) / 1000000 ))
echo "  mini-Hadoop: ${ELAPSED_MS}ms" | tee -a $RESULTS_DIR/m1-wordcount.txt

# --- R1: Binary Size ---
echo "=== R1: Binary Size ==="
echo "mini-Hadoop binaries:" | tee -a $RESULTS_DIR/r1-size.txt
ls -lh bin/ 2>/dev/null | tee -a $RESULTS_DIR/r1-size.txt
echo "Docker image:" | tee -a $RESULTS_DIR/r1-size.txt
docker images mini-hadoop --format "{{.Size}}" | tee -a $RESULTS_DIR/r1-size.txt

# --- R2: Memory Usage ---
echo "=== R2: Memory (Idle) ==="
docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}" | tee -a $RESULTS_DIR/r2-memory.txt

echo ""
echo "=== Benchmark Complete ==="
echo "Results saved to $RESULTS_DIR/"
```

---

## 5. Expected Results (Hypothesis)

Based on architectural differences:

### Where mini-Hadoop Should Win

| Area | Why |
|------|-----|
| **Startup time** | Go binary starts in <1s vs JVM warm-up 10-30s |
| **Memory footprint** | Go ~50MB vs JVM NameNode alone ~1-2GB |
| **Binary size** | ~50MB total vs ~500MB+ for Hadoop distribution |
| **Dead node detection** | 10s default timeout vs Hadoop's 10m30s default |
| **Build time** | ~5s vs ~5-10 minutes |
| **Small file metadata** | In-memory Go map vs JVM heap + GC pressure |
| **Lines of code** | 24K vs 2M (83x less to audit, maintain, debug) |

### Where Apache Hadoop Should Win

| Area | Why |
|------|-----|
| **Large file throughput** | Hadoop uses native C libraries (libhadoop, libsnappy) for I/O, mini-Hadoop is pure Go |
| **Shuffle performance** | Hadoop's HTTP-based shuffle with Netty is heavily optimized; mini-Hadoop uses basic gRPC |
| **Concurrent clients** | Hadoop's NameNode RPC handler pool is battle-tested for thousands of concurrent operations |
| **Compression** | Hadoop supports Snappy (fast), LZ4, Zstd natively; mini-Hadoop only has gzip |
| **Scale (>10 nodes)** | Hadoop tested at 10,000+ nodes; mini-Hadoop tested at 3 |
| **Speculative execution** | Hadoop retries slow tasks on other nodes; mini-Hadoop doesn't |
| **Data locality accuracy** | Hadoop's rack-aware placement is more sophisticated |

### Where They Should Be Comparable

| Area | Why |
|------|-----|
| **Output correctness** | Both implement the same MapReduce semantics |
| **3x replication durability** | Same block replication model |
| **Heartbeat-based fault detection** | Same architectural pattern |
| **Single-node throughput** | Go and Java are comparable for I/O-bound work |
| **Small cluster performance** | At 3 nodes, framework overhead is small relative to I/O |

---

## 6. Results Template

After running benchmarks, fill in this template:

### Storage (S1-S4)

| Benchmark | mini-Hadoop | Hadoop | Winner | Notes |
|-----------|------------|--------|--------|-------|
| S1: 100MB write (MB/s) | | | | |
| S1: 1GB write (MB/s) | | | | |
| S2: 100MB read (MB/s) | | | | |
| S2: 1GB read (MB/s) | | | | |
| S3: 1000 small files (files/s) | | | | |
| S4: 4-client concurrent (all correct?) | | | | |

### Fault Tolerance (F1-F4)

| Benchmark | mini-Hadoop | Hadoop | Winner | Notes |
|-----------|------------|--------|--------|-------|
| F1: Detection time (s) | ~11s | depends on config | | |
| F2: Files readable after 1 node death | 10/10 | 10/10 | Tie | |
| F3: Full recovery time (s) | | | | |
| F4: Survive 2-of-3 death | | | | |

### MapReduce (M1-M4)

| Benchmark | mini-Hadoop | Hadoop | Winner | Notes |
|-----------|------------|--------|--------|-------|
| M1: WordCount 100MB (s) | | | | |
| M1: WordCount 1GB (s) | | | | |
| M2: SumByKey 10M records (s) | | | | |
| M3: Sort 1GB (s) | | | | |
| M4: 3-node scaling efficiency | | | | |

### Resources (R1-R5)

| Benchmark | mini-Hadoop | Hadoop | Winner | Notes |
|-----------|------------|--------|--------|-------|
| R1: Total binary size | ~50MB | ~500MB | mini | ~10x smaller |
| R2: Idle memory (total) | ~50MB | ~2GB | mini | ~40x less |
| R3: Peak memory (1GB job) | | | | |
| R4: Startup time (s) | <2s | ~20s | mini | ~10x faster |
| R5: Lines of code | 24K | 2M | mini | 83x less |

### Correctness (C1-C3)

| Benchmark | mini-Hadoop | Hadoop | Match? |
|-----------|------------|--------|--------|
| C1: WordCount output | | | diff = 0 lines? |
| C2: Empty file | | | Both handle? |
| C2: 1M identical lines | | | Both correct? |
| C3: Replica consistency | | | All SHA-256 match? |

---

## 7. How to Reproduce

```bash
# 1. Clone and setup mini-Hadoop
git clone https://github.com/andyzengmath/mini-Hadoop.git
cd mini-Hadoop
make docker-build && make docker-up

# 2. Install Apache Hadoop (pseudo-distributed)
# Follow https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html

# 3. Generate benchmark data
python3 scripts/generate_bench_data.py  # (create this script)

# 4. Run benchmarks
bash doc/benchmark.sh  # (the script from Section 4)

# 5. Fill in results template (Section 6)
```

---

## 8. Presentation Format

For sharing results, use this structure:

```markdown
# mini-Hadoop vs Apache Hadoop: Benchmark Results

## TL;DR
- mini-Hadoop is Nx faster to start, uses Nx less memory, Nx smaller binary
- Apache Hadoop is Nx faster for large file I/O and shuffle
- Both produce identical MapReduce output
- mini-Hadoop detects node failures Nx faster

## Detailed Results
(tables from Section 6)

## Methodology
(describe hardware, software versions, run count, statistical method)

## Conclusion
(where each system excels, recommended use cases)
```
