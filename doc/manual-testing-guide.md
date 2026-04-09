# mini-Hadoop Manual Testing Guide

A step-by-step guide for testing all essential functions of mini-Hadoop. No prior Hadoop knowledge required.

---

## What is mini-Hadoop?

mini-Hadoop is a distributed computing system with three layers:

1. **HDFS (Storage)** — A distributed filesystem that splits files into 128MB blocks and stores 3 copies of each block across different machines. Like Google Drive, but the files are automatically replicated for safety.

2. **YARN (Resource Management)** — A job scheduler that decides which machine runs which task. Like a task manager that assigns work to workers.

3. **MapReduce (Processing)** — A framework for processing large files in parallel. "Map" transforms data, "Reduce" aggregates it. Like splitting a phone book among 10 people to count names, then combining their results.

---

## Prerequisites

- Docker Desktop installed and running
- Git (to clone the repo)
- A terminal (PowerShell, bash, or cmd)

### Quick Setup

```bash
# Clone the repo
git clone https://github.com/andyzengmath/mini-Hadoop.git
cd mini-Hadoop

# Build and start the 3-node cluster
docker build --no-cache -t mini-hadoop -f docker/Dockerfile .
docker compose -f docker/docker-compose.yml up -d

# Wait 10 seconds for services to register
sleep 10

# Verify the cluster is running (should show 6 containers)
docker compose -f docker/docker-compose.yml ps
```

**Expected output:** 6 containers running (namenode, resourcemanager, worker-1, worker-2, worker-3, client)

---

## Test 1: Cluster Health Check

**What we're testing:** All services started correctly and can communicate.

### Steps

```bash
# Check NameNode registered 3 DataNodes
docker compose -f docker/docker-compose.yml logs namenode | grep "DataNode registered"
```

**Expected:** Three lines showing worker-1, worker-2, worker-3 registered:
```
DataNode registered  nodeID=worker-1  address=worker-1:9001
DataNode registered  nodeID=worker-2  address=worker-2:9001
DataNode registered  nodeID=worker-3  address=worker-3:9001
```

```bash
# Check ResourceManager registered 3 NodeManagers
docker compose -f docker/docker-compose.yml logs resourcemanager | grep "NodeManager registered"
```

**Expected:** Three lines showing 3 workers with 4096MB memory each.

```bash
# Check NameNode metrics dashboard
docker compose -f docker/docker-compose.yml exec client \
  curl -s http://namenode:9100/metrics
```

**Expected:** JSON showing `"alive_datanodes":3`
```json
{"component":"NameNode","metrics":{"alive_datanodes":3,"namespace_entries":1,"total_blocks":0,"total_datanodes":3,"under_replicated":0}}
```

```bash
# Check ResourceManager metrics
docker compose -f docker/docker-compose.yml exec client \
  curl -s http://resourcemanager:9110/metrics
```

**Expected:** JSON showing `"alive_nodes":3` and `"total_memory_mb":12288` (3 nodes × 4096MB)

### Pass criteria
- [ ] 3 DataNodes registered
- [ ] 3 NodeManagers registered
- [ ] NameNode metrics show 3 alive datanodes
- [ ] ResourceManager metrics show 3 alive nodes

---

## Test 2: Create Directories

**What we're testing:** The filesystem namespace (directory tree) works like a regular filesystem.

### Steps

```bash
docker compose -f docker/docker-compose.yml exec client sh -c '
echo "=== Create a directory ==="
hdfs mkdir /data
hdfs ls /

echo ""
echo "=== Create nested directories ==="
hdfs mkdir /data/input
hdfs mkdir /data/output
hdfs ls /data
'
```

**Expected:**
```
Created directory: /data
d  r=0             0  data

Created directory: /data/input
Created directory: /data/output
d  r=0             0  input
d  r=0             0  output
```

### What's happening behind the scenes
The `hdfs mkdir` command sends a gRPC request to the NameNode, which creates the directory entry in its in-memory namespace tree. The directory exists only as metadata — no data blocks are involved yet.

### Pass criteria
- [ ] Directories created without errors
- [ ] `hdfs ls` shows the directories with `d` (directory) prefix

---

## Test 3: Upload and Download a Small File

**What we're testing:** The complete write/read pipeline: client → NameNode (metadata) → DataNode pipeline (3x replication) → read back.

### Steps

```bash
docker compose -f docker/docker-compose.yml exec client sh -c '
echo "=== Create a test file ==="
echo "Hello from mini-Hadoop! This is a test file." > /tmp/hello.txt
cat /tmp/hello.txt

echo ""
echo "=== Upload to HDFS ==="
hdfs put /tmp/hello.txt /data/input/hello.txt

echo ""
echo "=== Check file info ==="
hdfs info /data/input/hello.txt

echo ""
echo "=== List directory ==="
hdfs ls /data/input

echo ""
echo "=== Download from HDFS ==="
hdfs get /data/input/hello.txt /tmp/hello_downloaded.txt
cat /tmp/hello_downloaded.txt

echo ""
echo "=== Verify content matches ==="
diff /tmp/hello.txt /tmp/hello_downloaded.txt && echo "SUCCESS: Files match!" || echo "FAILURE: Files differ!"
'
```

**Expected:**
```
Hello from mini-Hadoop! This is a test file.
Uploaded /tmp/hello.txt -> /data/input/hello.txt
Path:        /data/input/hello.txt
Type:        file
Replication: 3
Hello from mini-Hadoop! This is a test file.
SUCCESS: Files match!
```

### What's happening behind the scenes
1. **Upload (put):** Client asks NameNode to create the file → NameNode picks 3 DataNodes → Client streams data through a pipeline (DN1 → DN2 → DN3) → All 3 nodes store a copy
2. **Download (get):** Client asks NameNode for block locations → Client reads from the nearest DataNode → Data streamed back to client

### Pass criteria
- [ ] File uploaded without errors
- [ ] File shows `Replication: 3`
- [ ] Downloaded content matches original

---

## Test 4: Upload a Large File with SHA-256 Verification

**What we're testing:** Data integrity across the distributed pipeline for a file larger than a single chunk (1MB). The file goes through multiple gRPC streaming chunks.

### Steps

```bash
docker compose -f docker/docker-compose.yml exec client sh -c '
echo "=== Generate 20MB random file ==="
dd if=/dev/urandom bs=1M count=20 2>/dev/null | base64 > /tmp/bigfile.txt
ls -lh /tmp/bigfile.txt
HASH_BEFORE=$(sha256sum /tmp/bigfile.txt | awk "{print \$1}")
echo "SHA-256 before: $HASH_BEFORE"

echo ""
echo "=== Upload to HDFS ==="
hdfs put /tmp/bigfile.txt /data/input/bigfile.txt

echo ""
echo "=== Download from HDFS ==="
hdfs get /data/input/bigfile.txt /tmp/bigfile_downloaded.txt

echo ""
echo "=== Verify SHA-256 ==="
HASH_AFTER=$(sha256sum /tmp/bigfile_downloaded.txt | awk "{print \$1}")
echo "SHA-256 after:  $HASH_AFTER"

if [ "$HASH_BEFORE" = "$HASH_AFTER" ]; then
  echo "SUCCESS: SHA-256 checksums match! Data integrity verified."
else
  echo "FAILURE: SHA-256 mismatch! Data corrupted during transfer."
fi
'
```

**Expected:** Both SHA-256 hashes are identical.

### What's happening behind the scenes
The 27MB file (20MB raw → base64 encoded) is split into 1MB chunks, streamed through the 3-node DataNode pipeline, stored on disk, then read back and reassembled. SHA-256 verification proves not a single byte was lost or corrupted.

### Pass criteria
- [ ] File uploaded and downloaded without errors
- [ ] SHA-256 hashes match exactly

---

## Test 5: Fault Tolerance — Survive a Node Failure

**What we're testing:** When a worker dies, the data is still accessible because it was replicated to 3 nodes. The NameNode detects the failure and schedules re-replication.

### Steps

```bash
# Step 1: Upload a file (if not already done from Test 4)
docker compose -f docker/docker-compose.yml exec client sh -c '
echo "test data for fault tolerance" > /tmp/fault_test.txt
hdfs put /tmp/fault_test.txt /data/input/fault_test.txt
echo "File uploaded with 3x replication"
'

# Step 2: Kill worker-2
echo "=== Killing worker-2 ==="
docker compose -f docker/docker-compose.yml stop worker-2
echo "worker-2 is now dead"

# Step 3: Wait for NameNode to detect the failure (10-15 seconds)
echo "Waiting 15 seconds for failure detection..."
sleep 15

# Step 4: Check NameNode logs for dead node detection
echo "=== NameNode logs ==="
docker compose -f docker/docker-compose.yml logs namenode | grep -E "(dead|DEAD|marked dead)" | tail -3

# Step 5: Verify file is still readable!
echo ""
echo "=== Can we still read the file? ==="
docker compose -f docker/docker-compose.yml exec client sh -c '
hdfs get /data/input/fault_test.txt /tmp/fault_verify.txt
cat /tmp/fault_verify.txt
diff /tmp/fault_test.txt /tmp/fault_verify.txt && echo "SUCCESS: File still readable after node failure!" || echo "FAILURE: Data lost!"
'

# Step 6: Restart worker-2
echo ""
echo "=== Restarting worker-2 ==="
docker compose -f docker/docker-compose.yml start worker-2
echo "worker-2 is back"
```

**Expected:**
```
Killing worker-2...
worker-2 is now dead
Waiting 15 seconds for failure detection...
NameNode logs: DataNode marked dead  nodeID=worker-2
Can we still read the file?
test data for fault tolerance
SUCCESS: File still readable after node failure!
Restarting worker-2...
worker-2 is back
```

### What's happening behind the scenes
1. The file was stored on 3 DataNodes (worker-1, worker-2, worker-3)
2. When worker-2 dies, the NameNode notices missing heartbeats after ~10 seconds
3. The NameNode marks worker-2 as dead and removes it from block locations
4. When the client reads the file, it gets block locations [worker-1, worker-3] (worker-2 excluded)
5. The client reads from worker-1 or worker-3 — the file is intact
6. The NameNode also schedules re-replication to restore the 3rd copy

### Pass criteria
- [ ] NameNode detects dead node within 15 seconds
- [ ] File is still readable after node death
- [ ] Downloaded content matches original

---

## Test 6: MapReduce WordCount

**What we're testing:** The core MapReduce pipeline: split input → parallel map → sort → shuffle → reduce → output.

### Steps

```bash
docker compose -f docker/docker-compose.yml exec client sh -c '
echo "=== Create test input ==="
cat > /tmp/wordcount_input.txt << INPUTEOF
the quick brown fox jumps over the lazy dog
the fox jumps high and the dog sleeps
quick brown fox quick brown fox
INPUTEOF
cat /tmp/wordcount_input.txt
echo ""

echo "=== Run WordCount MapReduce job ==="
mapreduce --job wordcount --input /tmp/wordcount_input.txt --output /tmp/wc_output --local

echo ""
echo "=== WordCount Results ==="
echo "(word → count)"
sort -t"	" -k2 -rn /tmp/wc_output/part-00000
echo ""

echo "=== Verify total word count ==="
TOTAL=$(awk -F"\t" "{sum+=\$2} END {print sum}" /tmp/wc_output/part-00000)
echo "Total words: $TOTAL"

# Count words manually for verification
EXPECTED=$(wc -w < /tmp/wordcount_input.txt)
echo "Expected:    $EXPECTED"

if [ "$TOTAL" = "$EXPECTED" ]; then
  echo "SUCCESS: Word counts match!"
else
  echo "FAILURE: Word counts differ!"
fi
'
```

**Expected:**
```
=== WordCount Results ===
(word → count)
the     4
fox     3
quick   3
brown   3
...
Total words: 27
Expected:    27
SUCCESS: Word counts match!
```

### What's happening behind the scenes
1. **Map phase:** Each line is split into words. For each word, emit `(word, 1)`.
   - "the quick brown fox" → `(the,1) (quick,1) (brown,1) (fox,1)`
2. **Sort phase:** All pairs sorted by key: `(brown,1) (brown,1) (brown,1) (dog,1) (dog,1) ...`
3. **Reduce phase:** Group by key and sum: `(brown, [1,1,1]) → (brown, 3)`

### Pass criteria
- [ ] Job completes without errors
- [ ] Each word has the correct count
- [ ] Total words match

---

## Test 7: Distributed MapReduce with Mapworker Binary

**What we're testing:** The mapworker binary running map and reduce as separate processes (like real Hadoop), with partitioned shuffle between them.

### Steps

```bash
docker compose -f docker/docker-compose.yml exec client sh -c '
echo "=== Generate larger input ==="
words="hadoop distributed computing mapreduce namenode datanode"
i=0
while [ $i -lt 1000 ]; do
  echo "$words"
  i=$((i+1))
done > /tmp/mr_large.txt
echo "Input: $(wc -l < /tmp/mr_large.txt) lines, $(wc -w < /tmp/mr_large.txt) words"

echo ""
echo "=== Run Map phase (2 reducers) ==="
mapworker --mode map --task-id map-0 --job-id test-7 \
  --mapper wordcount --input /tmp/mr_large.txt --num-reducers 2

echo ""
echo "=== Run Reduce phase (partition 0) ==="
mapworker --mode reduce --task-id red-0 --job-id test-7 \
  --reducer wordcount --partition-id 0 --output /tmp/reduce_out_0.txt

echo ""
echo "=== Run Reduce phase (partition 1) ==="
mapworker --mode reduce --task-id red-1 --job-id test-7 \
  --reducer wordcount --partition-id 1 --output /tmp/reduce_out_1.txt

echo ""
echo "=== Results from Reducer 0 ==="
cat /tmp/reduce_out_0.txt

echo ""
echo "=== Results from Reducer 1 ==="
cat /tmp/reduce_out_1.txt

echo ""
echo "=== Verify total ==="
TOTAL=$(cat /tmp/reduce_out_0.txt /tmp/reduce_out_1.txt | awk -F"\t" "{sum+=\$2} END {print sum}")
echo "Total word count: $TOTAL (expected: 6000 = 6 words × 1000 lines)"
if [ "$TOTAL" = "6000" ]; then
  echo "SUCCESS: Distributed MapReduce correct!"
else
  echo "FAILURE: Expected 6000, got $TOTAL"
fi
'
```

**Expected:**
- Each of 6 words appears exactly 1000 times
- Words are partitioned across 2 reducers by hash
- Total = 6000

### What's happening behind the scenes
1. **Map task:** Reads input, applies WordCount mapper, sorts output by (partition, key), writes to partition files
2. **Reduce task 0:** Reads partition-0 data from map output, groups by key, sums values
3. **Reduce task 1:** Same for partition-1 data
4. Words are distributed across partitions by `hash(word) % 2`

### Pass criteria
- [ ] Map phase completes, creates 2 partition files
- [ ] Both reduce phases complete
- [ ] Each word has count 1000
- [ ] Total across both reducers = 6000

---

## Test 8: File Operations (Delete, List, Info)

**What we're testing:** Full HDFS namespace operations — not just write/read but also delete and directory listing.

### Steps

```bash
docker compose -f docker/docker-compose.yml exec client sh -c '
echo "=== Current files ==="
hdfs ls /data/input

echo ""
echo "=== File info ==="
hdfs info /data/input/hello.txt

echo ""
echo "=== Delete a file ==="
hdfs rm /data/input/hello.txt
echo "Deleted hello.txt"

echo ""
echo "=== Verify deletion ==="
hdfs ls /data/input

echo ""
echo "=== Delete directory recursively ==="
hdfs rm /data/output
echo "Deleted /data/output"

echo ""
echo "=== Final directory listing ==="
hdfs ls /data
'
```

### Pass criteria
- [ ] File info shows correct metadata
- [ ] File deletion succeeds
- [ ] Deleted file no longer appears in listing
- [ ] Recursive directory deletion works

---

## Test 9: Metrics Dashboard

**What we're testing:** The web dashboard shows real-time cluster metrics.

### Steps

```bash
# After running previous tests, check metrics reflect the activity:
docker compose -f docker/docker-compose.yml exec client sh -c '
echo "=== NameNode Metrics ==="
curl -s http://namenode:9100/metrics | python3 -m json.tool 2>/dev/null || \
  curl -s http://namenode:9100/metrics

echo ""
echo "=== ResourceManager Metrics ==="
curl -s http://resourcemanager:9110/metrics | python3 -m json.tool 2>/dev/null || \
  curl -s http://resourcemanager:9110/metrics

echo ""
echo "=== Health Checks ==="
echo -n "NameNode health: "
curl -s http://namenode:9100/health
echo ""
echo -n "RM health: "
curl -s http://resourcemanager:9110/health
'
```

**Expected:** NameNode shows blocks > 0 (from uploaded files), both health checks return `{"status":"ok"}`

### Pass criteria
- [ ] NameNode metrics show total_blocks > 0
- [ ] Both health endpoints return `{"status":"ok"}`

---

## Test 10: SumByKey (Alternative MapReduce Job)

**What we're testing:** A different MapReduce job type to verify the framework is generic, not hardcoded for WordCount.

### Steps

```bash
docker compose -f docker/docker-compose.yml exec client sh -c '
echo "=== Create key-value input ==="
cat > /tmp/sales.txt << SALESEOF
electronics	150
clothing	80
electronics	200
food	50
clothing	120
food	75
electronics	300
food	25
SALESEOF
cat /tmp/sales.txt

echo ""
echo "=== Run SumByKey MapReduce ==="
mapreduce --job sumbykey --input /tmp/sales.txt --output /tmp/sales_output --local

echo ""
echo "=== Results (category → total sales) ==="
sort -t"	" -k2 -rn /tmp/sales_output/part-00000
echo ""

echo "=== Verify ==="
echo "Expected: electronics=650, clothing=200, food=150"
'
```

**Expected:**
```
electronics	650
clothing	200
food	150
```

### What's happening behind the scenes
- **SumByKeyMapper:** Reads tab-separated `key\tvalue` lines, emits them as-is
- **SumReducer:** For each key, sums all integer values
- Result: total sales per category

### Pass criteria
- [ ] Electronics = 150 + 200 + 300 = 650
- [ ] Clothing = 80 + 120 = 200
- [ ] Food = 50 + 75 + 25 = 150

---

## Cleanup

```bash
# Stop the cluster
docker compose -f docker/docker-compose.yml down -v

# Remove the Docker image (optional)
docker rmi mini-hadoop
```

---

## Troubleshooting

### Cluster won't start
```bash
# Check Docker is running
docker version

# Rebuild from scratch
docker compose -f docker/docker-compose.yml down -v
docker build --no-cache -t mini-hadoop -f docker/Dockerfile .
docker compose -f docker/docker-compose.yml up -d
```

### DataNodes not registering
```bash
# Check worker logs for connection errors
docker compose -f docker/docker-compose.yml logs worker-1 | head -20
```
Common cause: NameNode not ready yet. Wait 10 seconds and check again.

### File upload fails
```bash
# Check NameNode logs for errors
docker compose -f docker/docker-compose.yml logs namenode | tail -20
```
Common cause: Not enough DataNodes alive for 3x replication (need at least 3).

### MapReduce job fails
```bash
# Run with full logging
docker compose -f docker/docker-compose.yml exec client sh -c '
mapreduce --job wordcount --input /tmp/test.txt --output /tmp/out --local 2>&1
'
```

---

## Test Summary Checklist

| # | Test | What it proves | Pass? |
|---|------|---------------|-------|
| 1 | Cluster Health | All 6 services running and communicating | ☐ |
| 2 | Create Directories | Namespace operations work | ☐ |
| 3 | Small File Upload/Download | Basic write/read pipeline | ☐ |
| 4 | Large File + SHA-256 | Data integrity across chunks | ☐ |
| 5 | Kill Worker Node | Fault tolerance via 3x replication | ☐ |
| 6 | WordCount MapReduce | Map → sort → reduce pipeline | ☐ |
| 7 | Distributed Mapworker | Multi-process map + reduce with partitioning | ☐ |
| 8 | File Operations | Delete, list, info commands | ☐ |
| 9 | Metrics Dashboard | Observability endpoints | ☐ |
| 10 | SumByKey Job | Generic MapReduce (not just WordCount) | ☐ |

**All 10 tests passing = mini-Hadoop is fully functional.**
