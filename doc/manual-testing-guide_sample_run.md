# mini-Hadoop Manual Testing Guide — Sample Run

**Date:** 2026-04-09
**Machine:** Windows 11 Enterprise, Docker Desktop 29.3.1
**Cluster:** 3-node Docker Compose (1 NameNode, 1 ResourceManager, 3 Workers, 1 Client)

---

## Test 1: Cluster Health Check — PASS

### DataNode Registration
```
namenode-1  | {"time":"2026-04-09T20:24:19.667Z","level":"INFO","msg":"DataNode registered","nodeID":"worker-3","address":"worker-3:9001"}
namenode-1  | {"time":"2026-04-09T20:24:19.739Z","level":"INFO","msg":"DataNode registered","nodeID":"worker-2","address":"worker-2:9001"}
namenode-1  | {"time":"2026-04-09T20:24:19.767Z","level":"INFO","msg":"DataNode registered","nodeID":"worker-1","address":"worker-1:9001"}
```

### NodeManager Registration
```
resourcemanager-1  | {"time":"2026-04-09T20:24:21.675Z","level":"INFO","msg":"NodeManager registered","nodeID":"worker-3","address":"worker-3:9011","memory_mb":4096,"cpu":4}
resourcemanager-1  | {"time":"2026-04-09T20:24:21.739Z","level":"INFO","msg":"NodeManager registered","nodeID":"worker-2","address":"worker-2:9011","memory_mb":4096,"cpu":4}
resourcemanager-1  | {"time":"2026-04-09T20:24:21.765Z","level":"INFO","msg":"NodeManager registered","nodeID":"worker-1","address":"worker-1:9011","memory_mb":4096,"cpu":4}
```

### NameNode Metrics
```json
{"component":"NameNode","metrics":{"alive_datanodes":3,"namespace_entries":1,"total_blocks":0,"total_datanodes":3,"under_replicated":0}}
```

### ResourceManager Metrics
```json
{"component":"ResourceManager","metrics":{"alive_nodes":3,"pending_requests":0,"running_apps":0,"total_apps":0,"total_memory_mb":12288,"total_nodes":3,"used_memory_mb":0}}
```

### Result: **PASS**
- [x] 3 DataNodes registered
- [x] 3 NodeManagers registered (4096MB each, 12288MB total)
- [x] NameNode metrics: alive_datanodes=3
- [x] ResourceManager metrics: alive_nodes=3

---

## Test 2: Create Directories — PASS

```
Created directory: /data
Created directory: /data/input
Created directory: /data/output
d  r=0             0  input
d  r=0             0  output
```

### Result: **PASS**
- [x] Directories created without errors
- [x] `hdfs ls` shows directories with `d` prefix

---

## Test 3: Small File Upload/Download — PASS

```
Uploaded /tmp/hello.txt -> /data/input/hello.txt
Downloaded /data/input/hello.txt -> /tmp/hello_downloaded.txt
```

`diff` output: (no output — files match)

### Result: **PASS**
- [x] File uploaded without errors
- [x] Downloaded content matches original (diff is empty)

---

## Test 4: Large File + SHA-256 Verification — PASS

```
SHA-256 before: 5f761693f8a91e200bbe7077dc206b368a29ae1b7c23178fe994d64197f97c85
Uploaded /tmp/bigfile.txt -> /data/input/bigfile.txt
Downloaded /data/input/bigfile.txt -> /tmp/bigfile_downloaded.txt
SHA-256 after:  5f761693f8a91e200bbe7077dc206b368a29ae1b7c23178fe994d64197f97c85
```

### Result: **PASS**
- [x] ~27MB file uploaded and downloaded
- [x] SHA-256 checksums match exactly: `5f761693...97c85`

---

## Test 5: Fault Tolerance — Survive a Node Failure — PASS

### Kill worker-2
```
Container docker-worker-2-1 Stopping
Container docker-worker-2-1 Stopped
```

### NameNode detects failure (11 seconds)
```json
{"time":"2026-04-09T20:25:33.857Z","level":"WARN","msg":"DataNode marked dead","nodeID":"worker-2","lastHeartbeat":"2026-04-09T20:25:22.740Z"}
```

### File still readable
```
Downloaded /data/input/fault_test.txt -> /tmp/fault_verify.txt
```

`diff` output: (no output — files match)

### Restart worker-2
```
Container docker-worker-2-1 Started
```

### Result: **PASS**
- [x] NameNode detected dead node within 15 seconds (actually 11s)
- [x] File still readable from surviving replicas
- [x] Downloaded content matches original

---

## Test 6: WordCount MapReduce — PASS

### Input
```
the quick brown fox jumps over the lazy dog
the fox jumps high and the dog sleeps
quick brown fox quick brown fox
```

### Output (sorted by count, descending)
```
the       4
fox       4
quick     3
brown     3
jumps     2
dog       2
sleeps    1
over      1
lazy      1
high      1
and       1
```

### Verification
```
Total: 23 (expected: 23)
```

### Result: **PASS**
- [x] Job completed without errors
- [x] Word counts correct (the=4, fox=4, quick=3, brown=3, ...)
- [x] Total words match: 23

---

## Test 7: Distributed MapReduce with Mapworker — PASS

### Input
```
1000 lines × 6 words = 6000 total words
Words: hadoop distributed computing mapreduce namenode datanode
```

### Map Phase
```json
{"level":"INFO","msg":"map task starting","task_id":"map-0","job_id":"test-7","mapper":"wordcount","num_reducers":2}
{"level":"INFO","msg":"map task complete","task_id":"map-0","partitions":2}
```

### Reduce Phase — Reducer 0
```
distributed   1000
hadoop        1000
namenode      1000
```

### Reduce Phase — Reducer 1
```
computing     1000
datanode      1000
mapreduce     1000
```

### Verification
```
Total: 6000 (expected: 6000)
```

### Result: **PASS**
- [x] Map phase created 2 partition files
- [x] Both reducers completed correctly
- [x] Each word has count 1000
- [x] Total across both reducers = 6000

---

## Test 8: File Operations (Delete, List, Info) — PASS

### List before delete
```
-  r=3             0  hello.txt
-  r=3             0  bigfile.txt
-  r=3             0  fault_test.txt
```

### File info
```
Path:        /data/input/hello.txt
Type:        file
Replication: 3
Blocks:      1
```

### Delete file
```
Deleted: /data/input/hello.txt
```

### List after delete
```
-  r=3             0  bigfile.txt
-  r=3             0  fault_test.txt
```

### Delete directory
```
Deleted: /data/output
```

### Result: **PASS**
- [x] File info shows correct metadata (Replication=3)
- [x] File deletion succeeds
- [x] Deleted file no longer appears in listing
- [x] Directory deletion works

---

## Test 9: Metrics Dashboard — PASS

### NameNode Metrics
```json
{"component":"NameNode","metrics":{"alive_datanodes":3,"namespace_entries":5,"total_blocks":2,"total_datanodes":3,"under_replicated":0}}
```

### ResourceManager Metrics
```json
{"component":"ResourceManager","metrics":{"alive_nodes":3,"pending_requests":0,"running_apps":0,"total_apps":0,"total_memory_mb":12288,"total_nodes":3,"used_memory_mb":0}}
```

### Health Checks
```json
NameNode:  {"status":"ok"}
RM:        {"status":"ok"}
```

### Result: **PASS**
- [x] NameNode metrics show total_blocks=2 (from uploaded files)
- [x] Both health endpoints return `{"status":"ok"}`

---

## Test 10: SumByKey (Alternative MapReduce Job) — PASS

### Input (sales data)
```
electronics   150
clothing       80
electronics   200
food           50
clothing      120
food           75
electronics   300
food           25
```

### Output
```
clothing      200
electronics   650
food          150
```

### Verification
```
electronics=650 (expected 650) ✓
clothing=200 (expected 200) ✓
food=150 (expected 150) ✓
```

### Result: **PASS**
- [x] Electronics = 150 + 200 + 300 = 650
- [x] Clothing = 80 + 120 = 200
- [x] Food = 50 + 75 + 25 = 150

---

## Summary

| # | Test | Result |
|---|------|--------|
| 1 | Cluster Health Check | **PASS** |
| 2 | Create Directories | **PASS** |
| 3 | Small File Upload/Download | **PASS** |
| 4 | Large File + SHA-256 | **PASS** |
| 5 | Fault Tolerance (kill worker) | **PASS** |
| 6 | WordCount MapReduce | **PASS** |
| 7 | Distributed Mapworker (2 reducers) | **PASS** |
| 8 | File Operations (delete, list, info) | **PASS** |
| 9 | Metrics Dashboard | **PASS** |
| 10 | SumByKey MapReduce | **PASS** |

**10/10 PASS — mini-Hadoop is fully functional.**
