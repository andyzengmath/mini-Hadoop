# mini-Hadoop: Next Stage Plan

**Date:** 2026-04-09
**Base:** PR #1 (`0eb4b68`) — core implementation + 6 critical fixes
**Sources:** PR review findings (14 improvements remaining), comprehensive report Section 12, fix-plan-v1 remaining items

---

## Stage Overview

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                        mini-Hadoop Next Stage Roadmap                            │
│                                                                                  │
│  Stage A: Harden & Test        Stage B: Distributed MR      Stage C: Optimize   │
│  (Quality gate)                (Feature complete)            (Performance)        │
│                                                                                  │
│  ┌────────────────────┐   ┌────────────────────────┐   ┌──────────────────────┐ │
│  │ A1: Unit tests for │   │ B1: Streaming block    │   │ C1: Short-circuit    │ │
│  │     core packages  │   │     writes (no buffer) │   │     local reads      │ │
│  │ A2: PR improvement │   │ B2: Ack-queue pipeline │   │ C2: Combiner support │ │
│  │     findings       │   │ B3: Distributed MR E2E │   │ C3: Block compression│ │
│  │ A3: Docker cluster │   │ B4: All 7 ACs pass on  │   │ C4: Metrics/OpenTel  │ │
│  │     AC validation  │   │     Docker cluster     │   │ C5: Raw TCP transport│ │
│  └────────────────────┘   └────────────────────────┘   └──────────────────────┘ │
│          │                          │                           │                │
│     ~2-3 days                  ~3-5 days                   ~5-7 days             │
│                                                                                  │
│  ┌──────────────────────────────────────────────────────────────────────────────┐│
│  │              Stage D: Enterprise Features (Long-term)                        ││
│  │  D1: HA NameNode    D2: Capacity Scheduler   D3: Erasure Coding            ││
│  │  D4: Spark-like Engine   D5: Web UI                                         ││
│  └──────────────────────────────────────────────────────────────────────────────┘│
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

## Stage A: Harden & Test (Quality Gate)

**Goal:** Bring test coverage to production-grade and fix all remaining improvement findings from the PR review. No new features — only strengthening what exists.

**Gate:** `go test -race -cover ./pkg/... ./test/...` shows >70% coverage on core packages, all 28+ tests pass.

### A1: Unit Tests for Core Server Packages (~500 lines of new tests)

| Package | Priority | Test Cases | Est. Lines |
|---------|----------|-----------|------------|
| `pkg/namenode/blockmanager.go` | P0 | AllocateBlock (not enough nodes, capacity sorting), DetectDeadNodes (timeout boundary), CheckAndReplicateBlocks (schedule + pending tracking), ProcessBlockReport (orphan detection, pending confirmation) | ~120 |
| `pkg/namenode/namespace.go` | P0 | MkDir (with/without parents, error cases), CreateFile (duplicate, missing parent), CompleteFile (state transition), Delete (recursive block collection, non-empty dir), AddBlockToFile | ~100 |
| `pkg/namenode/persistence.go` | P0 | SaveState + LoadState round-trip, RestoreNamespace (nil input, full tree), non-existent file returns nil | ~80 |
| `pkg/datanode/storage.go` | P0 | WriteBlock + ReadBlock + DeleteBlock, path traversal rejection, scan-on-startup, HasBlock, GetBlockReport | ~80 |
| `pkg/resourcemanager/scheduler.go` | P1 | SubmitApp (no nodes fails), AM container allocation, UpdateContainerStatus (AM failure → app FAILED), double-free prevention, locality preference, pending queue fulfillment | ~100 |
| `pkg/nodemanager/server.go` | P1 | validateID (valid/invalid IDs), safePath (traversal rejection), validateCommand (allowlist), validateArgs (flag allowlist), LaunchContainer rejection cases | ~60 |

### A2: Fix Remaining PR Review Improvement Findings

| # | Finding | File | Fix |
|---|---------|------|-----|
| I1 | Off-by-one: MaxAttempts+1 total executions | `appmaster.go:197` | Rename to `MaxRetries` or change loop bound to `<` |
| I2 | No context timeout on NM status poll | `appmaster.go:242` | Add `context.WithTimeout(5s)` per GetContainerStatus call |
| I3 | ReleaseContainers + UpdateContainerStatus double-free | `scheduler.go:246` | Check `c.State == "RUNNING"` before subtracting in ReleaseContainers |
| I4 | TransferBlock always returns Success:true | `datanode/server.go:378` | Refactor replicateBlock to return error |
| I5 | GenerationStamp hardcoded to 1 | `client.go:161` | Thread genStamp from AddBlock response through writeBlockToPipeline |
| I6 | Heartbeat error silently discarded | `namenode/server.go:234` | Add slog.Warn on error |
| I7 | Start() return type inconsistent | multiple | Make NameNode/RM Start() return error |
| I8 | Env var injection incomplete blocklist | `nodemanager/server.go` | Already fixed in 0eb4b68 (switched to allowlist) |
| I9 | Docker ports published to all interfaces | `docker-compose.yml` | Restrict to `127.0.0.1:port:port` |
| I10 | Docker containers no resource limits | `docker-compose.yml` | Add `deploy.resources.limits` |
| I11 | AC-7 stub with no assertions | `ac7_linear_scaling_test.go` | Add throughput lower bound + consistency check |
| I12 | SortBuffer test no sorted-order check | `mapreduce_test.go` | Add sort verification + spill trigger test |
| I13 | validateID/safePath not unit-tested | `nodemanager/server.go` | Covered by A1 above |
| I14 | AC-1 no in-process fallback | `ac1_file_readwrite_test.go` | Add lightweight loopback integration test |

### A3: Docker Cluster Acceptance Validation

Run the existing acceptance tests on the Docker Compose cluster:

| AC | Test | Docker Command |
|----|------|----------------|
| AC-1 | File write/read SHA-256 (already verified manually) | Automate in test harness |
| AC-2 | Block replication recovery | `docker compose stop worker-2` → poll block locations → verify 3/3 within 30s |
| AC-3 | WordCount on Docker cluster | Submit via `cmd/mapreduce` CLI in distributed mode |
| AC-4 | Data locality >70% | Parse MRAppMaster structured logs for locality_stats |
| AC-5 | Node failure mid-job | Kill worker during MR job → verify job completes |
| AC-6 | Shuffle correctness on cluster | Multi-node SumByKey with actual shuffle over network |
| AC-7 | Linear scaling | Run WordCount on 1/2/3 workers, measure throughput |

---

## Stage B: Distributed MR End-to-End (Feature Complete)

**Goal:** MapReduce jobs run fully distributed on the Docker cluster — from job submission through map/shuffle/reduce to output on HDFS.

**Gate:** AC-3 through AC-7 all pass on 3-node Docker cluster.

### B1: Streaming Block Writes

**Problem:** DataNode WriteBlock buffers entire 128MB block in `bytes.Buffer` before writing to disk. This uses O(block_size) memory per concurrent write.

**Fix:** Stream chunks directly to disk file via `io.TeeReader`. Forward chunks to pipeline as they arrive. Compute checksum incrementally with `hash.Hash`.

```
Current: recv chunks → buffer all → write file → forward all
    Memory: O(128MB) per write

Fixed:   recv chunk → write to file + forward + hash → repeat
    Memory: O(1MB) per write (chunk size)
```

**Files:** `pkg/datanode/server.go` (WriteBlock method)

### B2: Ack-Queue Pipeline Write with Failure Recovery

**Problem:** Current pipeline is fire-and-forget — if DN2 fails mid-write, the client doesn't know.

**Fix:** Implement the dual-queue pattern from real Hadoop:
- **Data queue:** Packets waiting to be sent
- **Ack queue:** Packets sent but not yet acknowledged
- On downstream failure: halt pipeline, drain ack queue back to data queue, request new pipeline from NameNode with new generation stamp

**Files:** `pkg/datanode/server.go`, `pkg/hdfs/client.go`

### B3: Distributed MapReduce End-to-End

Wire all components together for a full distributed MR job:

1. **MapReduce worker binary** (`cmd/jobs/mapworker/main.go`): A single binary that runs as either a map or reduce task based on `--mode` flag. Reads input from HDFS, writes output locally (map) or to HDFS (reduce).

2. **MRAppMaster binary** (`cmd/jobs/mrappmaster/main.go`): Wrapper that runs the `pkg/mapreduce/appmaster.go` logic as a standalone process launched by the NodeManager.

3. **Integration flow:**
```
Client CLI → RM (SubmitApp) → NM launches MRAppMaster
  → MRAppMaster: ComputeSplits → request map containers with locality
  → NM launches mapworker --mode map (reads HDFS block, writes local partitions)
  → ShuffleService serves partitions to reducers
  → NM launches mapworker --mode reduce (fetches partitions, writes HDFS output)
  → MRAppMaster reports FINISHED to RM
  → Client CLI polls GetApplicationReport → sees FINISHED
```

4. **Test:** Run WordCount on 10MB file distributed across 3 workers, verify output matches reference.

### B4: All 7 Acceptance Criteria Pass on Docker

After B1-B3, run the full acceptance suite:
```bash
MINIHADOOP_CLUSTER=1 make docker-test
```

All 7 tests must pass. 3 consecutive green runs required before declaring feature-complete.

---

## Stage C: Optimize (Performance & Operational Quality)

**Goal:** Production-grade performance and observability.

### C1: Short-Circuit Local Reads

When a client reads a block that exists on the same node, bypass the DataNode gRPC call and read directly from the local filesystem via the block storage path. This eliminates network overhead for data-local reads.

**Implementation:**
- Add `localDataDir` field to HDFS client config
- Before calling DataNode ReadBlock, check if block file exists at `localDataDir/blockID.blk`
- If yes, read directly from disk (short-circuit)
- Measure: throughput improvement for data-local map tasks

### C2: Combiner Support (Pre-Aggregation)

Add optional Combiner interface that runs on map output before shuffle:
```go
type Combiner interface {
    Combine(key string, values []string, emit func(key, value string))
}
```
- Runs after each spill (in-memory, before writing to disk)
- Dramatically reduces shuffle volume for associative/commutative operations (e.g., WordCount sum)
- Measure: shuffle bytes before/after combiner on WordCount

### C3: Block Compression

Add transparent compression for stored blocks:
- Snappy for speed (default) or Zstd for ratio
- Compress on DataNode WriteBlock, decompress on ReadBlock
- Compression codec configurable per file via `CreateFileRequest`
- Measure: storage reduction and throughput impact

### C4: Metrics and Observability

Add Prometheus-compatible metrics to all components:

| Component | Key Metrics |
|-----------|------------|
| NameNode | total_blocks, under_replicated_blocks, live_datanodes, namespace_ops/sec |
| DataNode | stored_blocks, used_bytes, read_ops/sec, write_ops/sec, heartbeat_latency |
| ResourceManager | running_apps, pending_containers, allocated_memory, allocated_cpu |
| NodeManager | running_containers, available_memory, process_launches/sec |
| MapReduce | map_tasks_completed, reduce_tasks_completed, shuffle_bytes, data_local_percentage |

**Implementation:** Add `/metrics` HTTP endpoint to each component using `prometheus/client_golang`.

### C5: Raw TCP Transport (BlockTransport Swap)

Replace gRPC streaming with raw TCP for block data transfer:
- Implement `BlockTransport` interface with TCP sockets
- Custom framing: `[blockID_len][blockID][chunk_size][chunk_data][checksum]`
- Connection pooling between DataNodes
- Measure: throughput comparison gRPC vs raw TCP on 128MB blocks

---

## Stage D: Enterprise Features (Long-Term Vision)

### D1: HA NameNode

- Standby NameNode replicates edit log via shared journal
- ZooKeeper-based leader election (ZKFC)
- Automatic failover on active NameNode failure
- Client-side transparent failover (retry with new leader)

### D2: Capacity Scheduler

- Replace FIFO with queue-based scheduler
- Hierarchical queues with guaranteed minimum capacity
- Elastic burst into unused cluster capacity
- Preemption for high-priority applications
- Dominant Resource Fairness (DRF) for mixed CPU/memory workloads

### D3: Erasure Coding

- Replace 3x replication with Reed-Solomon (6+3) for storage-efficient fault tolerance
- 1.5x storage overhead vs 3x for same fault tolerance
- Block group management (6 data + 3 parity blocks)
- Degraded-mode reads when parity blocks needed

### D4: Spark-like In-Memory Processing Engine

- DAG execution engine on YARN-lite (not just MapReduce)
- In-memory intermediate data (no disk spill between stages)
- Broadcast joins, hash joins, sort-merge joins
- Lazy evaluation with query optimization

### D5: Web UI for Cluster Monitoring

- Real-time cluster dashboard (React + WebSocket)
- NameNode: namespace browser, block health, DataNode status
- ResourceManager: running applications, queue utilization, container allocation
- MapReduce: job progress, task timeline, locality visualization
- Integrated with Prometheus metrics from C4

---

## Implementation Priority Matrix

```
                    Impact
                    High ─────────────────────────────── Low
                    │                                     │
           Quick    │  A2 (improvement fixes)             │  A2-I6 (heartbeat log)
                    │  A2-I1 (retry off-by-one)           │  A2-I7 (Start() return)
                    │  A2-I2 (context timeout)            │
                    │  A2-I4 (TransferBlock error)        │
        Effort      │                                     │
                    │  A1 (unit tests)          ──────────│──── Nitpicks
                    │  B1 (streaming writes)              │
                    │  B3 (distributed MR E2E)            │
                    │  C1 (short-circuit reads)           │
           Large    │  B2 (ack-queue pipeline)            │
                    │  C4 (metrics)                       │
                    │  D1-D5 (enterprise features)        │
                    │                                     │
```

## Execution Order

```
Week 1:  Stage A (Harden & Test)
  Day 1: A1 — unit tests for namenode (blockmanager, namespace, persistence)
  Day 2: A1 — unit tests for datanode, scheduler, nodemanager
  Day 2: A2 — fix improvement findings I1-I7, I9-I12
  Day 3: A3 — Docker cluster AC validation (AC-1 through AC-7 where possible)

Week 2:  Stage B (Distributed MR)
  Day 4: B1 — streaming block writes
  Day 5: B2 — ack-queue pipeline
  Day 6-7: B3 — mapworker + mrappmaster binaries, end-to-end distributed MR
  Day 7: B4 — all 7 ACs pass on Docker cluster

Week 3+: Stage C (Optimize)
  C1: Short-circuit local reads
  C2: Combiner support
  C3: Block compression
  C4: Metrics and observability
  C5: Raw TCP transport

Future:  Stage D (Enterprise)
  D1-D5 as needed based on requirements
```

## Success Criteria

| Stage | Gate | Metric |
|-------|------|--------|
| A | Quality | >70% test coverage on core packages, 0 critical/high findings open |
| B | Feature | All 7 ACs pass on 3-node Docker cluster, 3 consecutive green runs |
| C | Performance | Data-local reads 2x faster with short-circuit, shuffle volume 50% less with combiner |
| D | Enterprise | HA failover <30s, capacity scheduler supports 3+ queues |

---

## Appendix: Full Issue Tracker

### Open Items (from PR review + report)

| ID | Severity | Category | Description | Stage |
|----|----------|----------|-------------|-------|
| I1 | improvement | correctness | MaxAttempts off-by-one | A2 |
| I2 | improvement | correctness | No context timeout on NM poll | A2 |
| I3 | improvement | correctness | ReleaseContainers double-free path | A2 |
| I4 | improvement | correctness | TransferBlock always Success:true | A2 |
| I5 | improvement | correctness | GenerationStamp hardcoded to 1 | A2 |
| I6 | improvement | consistency | Heartbeat error silently discarded | A2 |
| I7 | improvement | consistency | Start() return type inconsistent | A2 |
| I9 | improvement | security | Docker ports on all interfaces | A2 |
| I10 | improvement | security | Docker no resource limits | A2 |
| I11 | improvement | testing | AC-7 stub no assertions | A2 |
| I12 | improvement | testing | SortBuffer no sort-order check | A2 |
| I14 | improvement | testing | AC-1 no in-process fallback | A2 |
| T1 | critical | testing | Unit tests for 6 core packages | A1 |
| F1 | feature | performance | Streaming block writes | B1 |
| F2 | feature | performance | Ack-queue pipeline | B2 |
| F3 | feature | distributed | MapReduce E2E on cluster | B3 |
| F4 | feature | performance | Short-circuit local reads | C1 |
| F5 | feature | performance | Combiner support | C2 |
| F6 | feature | performance | Block compression | C3 |
| F7 | feature | observability | Metrics/Prometheus | C4 |
| F8 | feature | performance | Raw TCP transport | C5 |
