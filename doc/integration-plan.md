# mini-Hadoop: Library-to-Integration Plan

**Date:** 2026-04-09
**Base:** PR #5 merged (quality audit fixes)
**Scope:** Wire 6 library-only features into the running system

---

## Problem Statement

The quality audit found that 6 Phase 2 features exist as working library code with passing tests, but are never called by any running server or client. They are "dead" in production despite being "alive" in tests.

```
                    ┌─────────────────────┐
                    │   Library-Only      │
                    │   (tests pass,      │
                    │    servers ignore)   │
                    └─────┬───────────────┘
                          │
            ┌─────────────┼─────────────────┐
            ▼             ▼                 ▼
     ┌──────────┐  ┌──────────┐     ┌──────────┐
     │ Quick    │  │ Medium   │     │ Hard     │
     │ (< 1hr) │  │ (2-4 hrs)│     │ (1+ day) │
     └──────────┘  └──────────┘     └──────────┘
     Combiner      QueueManager     ErasureCodec
                   AckQueue         TCPTransport
                   Compression      DAGEngine cmd
```

---

## Integration Items (ordered by effort)

### I1: Combiner → RunMapTask (Quick, ~30 min)

**Current state:** `SortBuffer.SetCombiner()` exists, `SumCombiner` exists, `GetCombiner()` factory exists. None are called.

**What to wire:**
```
pkg/mapreduce/worker.go: RunMapTask()
  - After creating SortBuffer, check if combiner is requested
  - Call sb.SetCombiner(combiner) before processing input

pkg/mapreduce/worker.go: RunLocalJob()
  - Same: if cfg.CombinerName != "", get combiner and set on buffer

cmd/mapworker/main.go:
  - Accept --combiner flag, pass to RunMapTask

cmd/mapreduce/main.go:
  - Accept --combiner flag, pass in job submission
```

**Files changed:** 3 files, ~15 lines added
**Risk:** LOW — combiner reduces data volume, output is still correct (just fewer intermediate records)
**Test:** Modify TestRunLocalJob to use combiner, verify output unchanged but fewer spill records

---

### I2: QueueManager → FIFOScheduler (Medium, ~2 hrs)

**Current state:** `QueueManager` has `CanAllocate()`, `Allocate()`, `Release()`. `FIFOScheduler` ignores it entirely.

**What to wire:**

```
pkg/resourcemanager/scheduler.go:
  1. Add QueueManager field to FIFOScheduler
  2. NewFIFOScheduler() creates a QueueManager (default single queue or from config)
  3. allocateOne(): before allocating, call qm.CanAllocate(queueName, mem, cpu, totalClusterMem, totalClusterCPU)
     - If queue would exceed max capacity → return error (container stays pending)
  4. After allocating: call qm.Allocate(queueName, mem, cpu)
  5. In ReleaseContainers / UpdateContainerStatus: call qm.Release(queueName, mem, cpu)

pkg/resourcemanager/server.go:
  6. GetMetrics(): include queue stats from qm.ListQueues()

proto/resourcemanager.proto:
  7. Add optional 'queue' field to SubmitApplicationRequest
  8. Add queue info to GetApplicationReportResponse

pkg/mapreduce/appmaster.go:
  9. MRAppMaster sets queue name when requesting containers (default: "default")
```

**Wiring diagram:**
```
SubmitApp(queue="production")
  → FIFOScheduler.SubmitApp()
    → qm.CanAllocate("production", amMem, amCPU, totalMem, totalCPU)
    → if ok: allocateOne() + qm.Allocate("production", ...)
    → if not: app.State = FAILED ("queue capacity exceeded")

AllocateContainers(appID)
  → look up app's queue
  → qm.CanAllocate(queue, ...) before each allocation
  → qm.Allocate(queue, ...) after each allocation

ReleaseContainers / container COMPLETED
  → qm.Release(queue, mem, cpu)
```

**Files changed:** 3 files (~60 lines added), 1 proto file (2 fields)
**Risk:** MEDIUM — changes the allocation path; existing tests must still pass with default single-queue
**Test:**
- Existing scheduler tests pass (single "default" queue = same as FIFO)
- New test: 2 queues (60%/40%), submit apps that exceed one queue's capacity → verify rejection
- New test: queue metrics appear in GetMetrics()

---

### I3: AckQueue → HDFS Client Pipeline Write (Medium, ~3 hrs)

**Current state:** `AckQueue` has Enqueue/Dequeue/Acknowledge/DrainToDataQueue. HDFS `Client.writeBlockToPipeline()` sends chunks directly.

**What to wire:**

```
pkg/hdfs/client.go: writeBlockToPipeline()
  Current flow:
    for each chunk in data:
      stream.Send(chunk)
    stream.CloseAndRecv()

  New flow:
    aq := NewAckQueue()
    // Enqueue all chunks
    for each chunk in data:
      aq.Enqueue(chunk.Data, offset, isLast)

    // Send loop: dequeue → send → wait for implicit ack (stream continues)
    for p := aq.Dequeue(); p != nil; p = aq.Dequeue() {
      err := stream.Send(chunk from p)
      if err != nil {
        // Pipeline failure!
        drained := aq.DrainToDataQueue()
        slog.Warn("pipeline failure, drained packets", "count", drained)
        // For now: return error (future: rebuild pipeline and retry)
        return fmt.Errorf("pipeline write failed: %w", err)
      }
      aq.Acknowledge(p.SeqNo)
    }
    stream.CloseAndRecv()
```

**Why not full pipeline rebuild yet:** Pipeline rebuild requires:
1. New proto RPC: `NameNode.RebuildPipeline(blockID, newGenStamp, excludeNodes)`
2. NameNode to track in-flight blocks
3. Client to reconnect to new pipeline

This is a Phase 3 feature. For now, wiring the AckQueue gives us:
- Proper packet tracking (which chunks are sent vs acknowledged)
- DrainToDataQueue on failure (packets preserved for future retry)
- Foundation for pipeline rebuild (just need to add the retry loop later)

**Files changed:** 1 file (`client.go`), ~30 lines modified
**Risk:** MEDIUM — changes the hot write path; must verify file write/read still works
**Test:**
- All existing acceptance tests must pass (AC-1 file write/read)
- Docker cluster test: write 50MB file, verify SHA-256 match (same as before)

---

### I4: BlockCompression → DataNode Read/Write Path (Medium, ~3 hrs)

**Current state:** `CompressBlock()`, `DecompressBlock()`, `GzipCodec` exist. DataNode stores raw data.

**What to wire:**

```
Storage layer approach (transparent to client):
  DataNode compresses on write, decompresses on read.
  Client sees uncompressed data — no client changes needed.

pkg/datanode/storage.go:
  1. Add CompressionCodec field to BlockStorage
  2. WriteBlock(): compress data before writing to disk
     compressed, _ := codec.Compress(data)
     f.Write(compressed)
  3. ReadBlock(): decompress after reading from disk
     compressed := readFile(path)
     decompressed, _ := codec.Decompress(compressed)
     return decompressed

pkg/datanode/server.go (streaming WriteBlock):
  4. After writing all chunks to local file, compress the file
     OR: accumulate chunks, compress, write compressed version
  5. ReadBlock: read compressed file, decompress, stream chunks

pkg/config/config.go:
  6. Add CompressionCodec string field (default: "none")
  7. Env var: MINIHADOOP_COMPRESSION=gzip

Alternative: chunk-level compression (compress each 1MB chunk independently)
  - Pro: streaming-friendly, no need to buffer entire block
  - Con: worse compression ratio (can't exploit cross-chunk patterns)
  - Recommendation: block-level compression (simpler, better ratio)
```

**Wiring diagram:**
```
Client → WriteBlock chunks → DataNode
  DataNode writes chunks to temp file
  DataNode compresses temp file → block.blk.gz
  DataNode registers compressed block

Client ← ReadBlock chunks ← DataNode
  DataNode reads block.blk.gz
  DataNode decompresses → stream uncompressed chunks to client
```

**Files changed:** 3 files (~50 lines)
**Risk:** MEDIUM — changes storage format; existing blocks won't be readable if compression toggled mid-run
**Mitigation:** Store codec name in block metadata; fall back to raw read if decompression fails
**Test:**
- Write with compression=gzip, read back, verify SHA-256 match
- Write with compression=none, read back (backward compat)
- Benchmark: storage size reduction on text data

---

### I5: TCPTransport → DataNode + HDFS Client (Hard, ~1 day)

**Current state:** `TCPTransport` implements `BlockTransport` interface with wire protocol. No server listens on TCP.

**What to wire:**

```
Phase 1: DataNode TCP server
  pkg/datanode/tcpserver.go (new file):
    1. TCP listener on configurable port (default: 9002)
    2. Accept connections, read frames, dispatch to storage
    3. Handle WRITE_HEADER → receive DATA_CHUNKs → write to storage → send ACK
    4. Handle READ request → stream DATA_CHUNKs from storage → send ACK

  cmd/datanode/main.go:
    5. Start TCP server alongside gRPC server
    6. Config: MINIHADOOP_TCP_PORT=9002

Phase 2: HDFS Client transport selection
  pkg/hdfs/client.go:
    7. Add transport field: BlockTransport (gRPC or TCP)
    8. Config: MINIHADOOP_BLOCK_TRANSPORT=grpc|tcp
    9. writeBlockToPipeline(): delegate to transport.SendBlock()
    10. readBlockFromDataNode(): delegate to transport.ReceiveBlock()

Phase 3: Pipeline forwarding over TCP
  pkg/datanode/tcpserver.go:
    11. On WRITE with pipeline: forward frames to next DataNode via TCP
    12. ACK propagation back through TCP pipeline
```

**Files changed:** 2 new files + 3 modified (~200 lines)
**Risk:** HIGH — introduces a second data plane protocol; TCP bugs could corrupt data
**Mitigation:** Feature-flag behind config (default: gRPC); integration tests with both transports
**Test:**
- Write/read via TCP transport, verify SHA-256
- Benchmark: TCP vs gRPC throughput on 128MB block
- Mixed mode: some DataNodes on TCP, some on gRPC (should still work via fallback)

---

### I6: ErasureCodec → NameNode + HDFS Client (Hard, ~2 days)

**Current state:** `ErasureCodec.Encode()/Decode()` works for 6+3 RS. `BlockGroup` type exists. Nothing in NameNode or client uses them.

**What to wire:**

```
This is the most architecturally complex integration because it changes
the fundamental storage model from "one block = 3 replicas on 3 nodes"
to "one block group = 9 shards on 9 nodes".

Phase 1: NameNode EC policy support
  pkg/namenode/blockmanager.go:
    1. Add ECPolicy field to file metadata (per-directory, inherited)
    2. New method: AllocateBlockGroup(policy) → BlockGroup with 9 shards
       - Select 9 different DataNodes
       - Create 9 block IDs (6 data + 3 parity)
       - Return BlockGroup with shard-to-node mapping
    3. Under-replication detection for EC blocks:
       - If < 6 data shards available → block group is degraded
       - If any shard missing → schedule reconstruction

  proto/namenode.proto:
    4. Add ECPolicy to CreateFileRequest
    5. Add BlockGroup to GetBlockLocationsResponse
    6. New RPC: SetECPolicy(path, policy)

Phase 2: HDFS Client EC write
  pkg/hdfs/client.go:
    7. CreateFile with EC policy:
       - Buffer full block (128MB) in memory
       - Encode with ErasureCodec → 9 shards
       - Write each shard to its assigned DataNode in parallel
    8. CompleteFile: report all 9 block IDs

Phase 3: HDFS Client EC read
  pkg/hdfs/client.go:
    9. ReadFile with EC blocks:
       - Get BlockGroup from NameNode
       - Read any 6 shards (prefer local, then nearest)
       - Decode → original block data
    10. Degraded-mode read:
       - If < 6 data shards available, read all available + reconstruct
       - Use ErasureCodec.Decode() with nil entries for missing shards

Phase 4: Reconstruction on failure
  pkg/namenode/blockmanager.go:
    11. When DataNode dies, check EC block groups for missing shards
    12. Schedule reconstruction: read 6 available shards, recompute missing, write to new node
```

**Files changed:** 4 files modified + 1 proto change (~300 lines)
**Risk:** HIGH — fundamental storage model change; must not break 3x replication path
**Mitigation:**
- EC is per-file (opted-in via CreateFile policy), not global
- Default remains 3x replication — EC is additive, not replacing
- Parallel write to 9 nodes needs connection pooling and error handling
**Test:**
- Write EC file → read back → verify data correct
- Write EC file → kill 1 DataNode → read succeeds (degraded mode)
- Write EC file → kill 3 DataNodes → read still succeeds (max tolerance)
- Write EC file → kill 4 DataNodes → read fails (expected)
- Storage comparison: 3x replication vs EC on same data

---

### I7: DAGEngine → cmd binary + YARN integration (Hard, ~1 day)

**Current state:** `pkg/dagengine` has RDD + DAGScheduler. No cmd binary, no YARN integration.

**What to wire:**

```
Phase 1: Standalone DAG CLI
  cmd/dagrun/main.go (new binary):
    1. Accept --program flag pointing to a Go plugin or built-in job name
    2. Built-in programs: wordcount-dag, sum-dag
    3. Read input file → create RDD → execute DAG → write output
    4. Standalone mode (no YARN needed, like mapreduce --local)

Phase 2: YARN-integrated DAG execution
  pkg/dagengine/executor.go (new file):
    5. DAGExecutor runs on YARN as an ApplicationMaster
    6. For each stage:
       - Narrow deps: pipeline locally (same executor)
       - Wide deps: request containers from RM, launch shuffle tasks
    7. Shuffle between stages via ShuffleService

Phase 3: HDFS integration
  pkg/dagengine/rdd.go:
    8. NewRDDFromHDFS(client, path, numPartitions) → RDD from HDFS file
    9. SaveToHDFS(client, path) → write RDD partitions to HDFS
```

**Files changed:** 2 new files + 1 modified (~200 lines for Phase 1)
**Risk:** LOW for Phase 1 (standalone), HIGH for Phase 2 (YARN integration)
**Test:**
- DAG WordCount via CLI: same output as MapReduce WordCount
- Benchmark: DAG vs MapReduce on multi-stage job

---

## Execution Plan

```
Week 1 (Quick + Medium wins):
  Day 1: I1 Combiner (30 min) + I2 QueueManager (2 hrs) + I3 AckQueue (3 hrs)
  Day 2: I4 BlockCompression (3 hrs) + I7-Phase1 DAG CLI (2 hrs)
  Day 2: Update docs, commit, test on Docker

Week 2 (Hard integrations):
  Day 3-4: I5 TCPTransport (1 day)
  Day 5-6: I6 ErasureCodec (2 days)

Week 2+: I7-Phase2 DAG YARN integration (depends on I6)
```

## Priority Matrix

```
              Integration Impact
              High ────────────────── Low
              │                        │
    Quick     │ I1 Combiner            │
              │ I2 QueueManager        │
              │                        │
    Effort    │ I3 AckQueue            │
              │ I4 Compression         │
              │ I7 DAG CLI             │
              │                        │
    Hard      │ I5 TCP Transport       │
              │ I6 Erasure Coding      │
              │                        │
```

## Success Criteria

| Item | Gate |
|------|------|
| I1 | WordCount with combiner produces same output, fewer spill records |
| I2 | 2-queue setup rejects app exceeding queue capacity |
| I3 | 50MB file write/read with ack-queue, SHA-256 match |
| I4 | Compressed storage 50%+ smaller on text, read back correct |
| I5 | TCP block transfer >2x throughput vs gRPC |
| I6 | RS-6-3 file survives 3 node failures |
| I7 | DAG WordCount matches MapReduce WordCount output |

## Dependencies

```
I1 (Combiner) ──── independent
I2 (Queues) ────── independent
I3 (AckQueue) ──── independent
I4 (Compress) ──── independent
I5 (TCP) ────────── independent
I6 (Erasure) ───── independent (but benefits from I5 for parallel shard writes)
I7 (DAG CLI) ───── independent for Phase 1; Phase 2 needs ShuffleService registered
```

All items are independent — can be done in any order or in parallel.
