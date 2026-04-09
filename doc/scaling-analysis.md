# mini-Hadoop Scaling Analysis

**Date:** 2026-04-09
**Current verified scale:** 3 nodes (Docker Compose)
**Architecture:** Same master/worker topology as Apache Hadoop (scales to 10,000+ in production Hadoop)

---

## 1. Executive Summary

mini-Hadoop works well at 3-10 nodes. At 100+ nodes it would degrade. At 1000+ it would likely break. However, the architecture is fundamentally sound — the same master/worker pattern that real Hadoop uses at scale. The gaps are **implementation optimizations**, not architectural limitations. Scaling to 1000+ requires ~3 phases of engineering work totaling 4-6 weeks.

```
Current     Phase 1        Phase 2          Phase 3
3 nodes ──► 100 nodes ──► 500 nodes ──► 1000+ nodes
(today)     (2-3 days)    (1-2 weeks)    (1-2 months)
```

---

## 2. Current Scaling Limits

### 2.1 What Works at Any Scale (No Changes Needed)

| Component | Why It Scales |
|-----------|--------------|
| **DataNode storage** | Each DataNode is independent. Adding nodes is linear — no coordination between DataNodes except through NameNode. |
| **Pipeline replication** | Client → DN1 → DN2 → DN3 is peer-to-peer. No central bottleneck in the data path. Adding DataNodes doesn't slow existing pipelines. |
| **gRPC protocol** | HTTP/2 multiplexing handles many concurrent streams. Protobuf serialization is efficient regardless of cluster size. |
| **Block size (128MB)** | Large blocks mean fewer metadata entries per TB of data. 1PB of data = ~8M blocks, manageable in memory. |
| **Heartbeat protocol** | Same 3-second interval pattern as real Hadoop. The protocol itself scales — it's the processing that needs optimization. |
| **Worker process model** | Each worker runs independent DataNode + NodeManager processes. No cross-worker coordination overhead. |

### 2.2 Bottleneck Analysis

#### Bottleneck 1: Block Allocation Sorting — O(N²)
**File:** `pkg/namenode/blockmanager.go:getAliveNodesSorted()`
**Current:** Insertion sort of all alive DataNodes on every `AllocateBlock` call.
**Impact at scale:**
- 10 nodes: 100 comparisons (fine)
- 100 nodes: 10,000 comparisons (noticeable)
- 1000 nodes: 1,000,000 comparisons (blocks allocation)

```go
// Current: O(N²) insertion sort
func (bm *BlockManager) getAliveNodesSorted() []*DataNodeInfo {
    var alive []*DataNodeInfo
    // ... collect alive nodes ...
    for i := 1; i < len(alive); i++ {
        for j := i; j > 0 && alive[j].AvailableBytes > alive[j-1].AvailableBytes; j-- {
            alive[j], alive[j-1] = alive[j-1], alive[j]
        }
    }
    return alive
}
```

**Fix:** Use a heap (priority queue) that maintains sorted order incrementally. Allocation becomes O(log N) instead of O(N²).

```go
// Fixed: O(log N) per allocation
type DataNodeHeap []*DataNodeInfo
func (h DataNodeHeap) Len() int { return len(h) }
func (h DataNodeHeap) Less(i, j int) bool { return h[i].AvailableBytes > h[j].AvailableBytes }
// ... standard heap interface ...
// Pop top-3 nodes for replication pipeline
```

**Effort:** 1-2 hours

---

#### Bottleneck 2: Heartbeat Serialization — Single Mutex
**File:** `pkg/namenode/blockmanager.go:ProcessHeartbeat()`
**Current:** All heartbeats go through `bm.mu.Lock()` — one global mutex for all DataNode state.
**Impact at scale:**
- 3-second heartbeat interval
- N DataNodes = N/3 heartbeats per second
- 100 nodes = 33 heartbeats/sec through one lock
- 1000 nodes = 333 heartbeats/sec through one lock

At 333 heartbeats/sec, each heartbeat gets ~3ms to complete while holding the lock. If any heartbeat takes longer (e.g., generating replication commands), all others queue up.

**Fix options:**

Option A: Shard by node ID range
```go
type ShardedBlockManager struct {
    shards [16]*BlockManagerShard  // 16 shards, ~62 nodes each at 1000
    // Route by hash(nodeID) % 16
}
```

Option B: Read-write lock optimization
```go
// Most heartbeat processing is read-only (check liveness, return commands)
// Use RWMutex and only take write lock for state mutations
bm.mu.RLock()  // for reading DataNode state
// ... process heartbeat ...
bm.mu.RUnlock()

bm.mu.Lock()   // only for writing (updating timestamps, queuing commands)
// ... update state ...
bm.mu.Unlock()
```

Option C: Lock-free heartbeat counter with periodic batch processing
```go
// Heartbeats update an atomic timestamp
atomic.StoreInt64(&dn.lastHeartbeat, time.Now().UnixNano())
// Background goroutine periodically batch-processes all heartbeats
```

**Effort:** Option B: 2-4 hours. Option A: 1-2 days.

---

#### Bottleneck 3: Block Report Reconciliation — O(blocks × DataNodes)
**File:** `pkg/namenode/blockmanager.go:ProcessBlockReport()`
**Current:** Each block report iterates over all reported blocks, checking each against the global block map. With 1000 DataNodes reporting 10K blocks each = 10M lookups per report cycle.

**Fix:** Add a per-DataNode block index:
```go
type BlockManager struct {
    blocks    map[string]*block.Metadata         // blockID → metadata
    dnBlocks  map[string]map[string]bool          // nodeID → {blockID: true}
    // ...
}
```

Block report becomes: diff the reported set against `dnBlocks[nodeID]` — O(reported_blocks) not O(total_blocks).

**Effort:** 3-4 hours

---

#### Bottleneck 4: NameNode Metadata — Single Process Memory
**File:** `pkg/namenode/namespace.go`
**Current:** Entire namespace tree in one Go process. All metadata in `map[string]*FSNode`.
**Impact at scale:**
- Each file entry: ~200 bytes (name, blockIDs, replication, timestamps)
- 1M files: ~200MB RAM (manageable)
- 10M files: ~2GB RAM (getting tight)
- 100M files: ~20GB RAM (need federation)

Real Hadoop NameNode holds ~100M-500M files at large scale, using ~50-100GB heap. Go's memory efficiency is slightly better than JVM, but the fundamental limit is the same: one process can hold only so many file entries.

**Fix at 10M+ files:** NameNode Federation — split namespace into multiple NameNode shards, each managing a portion of the directory tree.

```
/user/alice   → NameNode A
/user/bob     → NameNode B
/data/logs    → NameNode C

Client mount table (ViewFS):
  /user/alice → nn-a:9000
  /user/bob   → nn-b:9000
  /data/*     → nn-c:9000
```

**Effort:** 2-4 weeks (major feature)

---

#### Bottleneck 5: FIFO Scheduler — Linear Scan
**File:** `pkg/resourcemanager/scheduler.go:allocateOne()`
**Current:** Iterates all nodes linearly to find one with available resources.
**Impact at scale:**
- 100 nodes × 100 pending requests = 10K iterations per allocation cycle
- With locality preferences, each request checks preferred nodes first (fast), then falls back to linear scan (slow)

**Fix:** Use indexed data structures:
```go
type IndexedScheduler struct {
    // Nodes indexed by available memory (skip list or balanced tree)
    byMemory *btree.BTree
    // Nodes indexed by address for O(1) locality lookup
    byAddress map[string]*NodeInfo
}
```

**Effort:** 4-6 hours

---

#### Bottleneck 6: Single Active NameNode
**Current:** Even with HA (edit log + election), only one NameNode serves requests.
**Impact at scale:**
- All metadata RPCs go through one process
- Read-heavy workloads (ListDirectory, GetBlockLocations) bottleneck on the active NameNode
- Write workloads (CreateFile, AddBlock) are serialized

**Fix — Phase 2:** Read-only NameNode replicas (Observer pattern)
```
Client ─── read RPCs ───► Observer NameNode (read-only, replays edit log)
Client ─── write RPCs ──► Active NameNode
```

**Fix — Phase 3:** Federation (multiple active NameNodes for different namespace partitions)

**Effort:** Observer: 1 week. Federation: 2-4 weeks.

---

## 3. Scaling Roadmap

### Phase 1: Get to ~100 Nodes (2-3 days)

Quick algorithmic fixes — no architectural changes.

| Fix | Current | After | Impact |
|-----|---------|-------|--------|
| Heap-based allocation | O(N²) sort | O(log N) heap pop | 100x faster at 1000 nodes |
| Per-DN block index | O(blocks) per report | O(reported) per report | 10x faster block reports |
| RWMutex for heartbeats | Write lock for all | Read lock for most | 3x more concurrent heartbeats |
| Priority queue scheduler | Linear scan | O(log N) allocation | 10x faster scheduling |

**Total effort:** 2-3 days
**Expected result:** Stable at 100 nodes, acceptable performance

### Phase 2: Get to ~500 Nodes (1-2 weeks)

Concurrency and caching optimizations.

| Fix | Description | Effort |
|-----|-------------|--------|
| Sharded heartbeat processing | 16 shards, each with own lock | 2 days |
| Batch block report reconciliation | Process reports in background goroutine | 1 day |
| Observer NameNode replicas | Read-only replicas for metadata reads | 3 days |
| Namespace caching | Cache hot directory listings | 1 day |
| Connection pool management | Limit concurrent gRPC streams per client | 1 day |

**Total effort:** 1-2 weeks
**Expected result:** Stable at 500 nodes, throughput scales linearly for data plane

### Phase 3: Get to 1000+ Nodes (1-2 months)

Architectural additions — these are the same features that Apache Hadoop added over years.

| Fix | Description | Effort |
|-----|-------------|--------|
| NameNode Federation | Multiple namespace shards | 2-3 weeks |
| Client ViewFS mount table | Automatic routing to correct NameNode | 1 week |
| Distributed block placement | Separate placement service | 1 week |
| GC tuning / memory profiling | Optimize Go GC for large heaps | 3-5 days |
| Network topology awareness | Rack-aware replica placement | 3-5 days |
| Benchmark at scale | Actual 1000-node testing | 1 week |

**Total effort:** 1-2 months
**Expected result:** Stable at 1000+ nodes, comparable to Hadoop at this scale

---

## 4. Scaling Math

### NameNode Memory Budget

```
Per-file overhead:  ~200 bytes (path + blockIDs + metadata)
Per-block overhead: ~150 bytes (blockID + locations + checksum + stamps)

128MB block size → 1TB of data = 8,192 blocks

Cluster: 1000 nodes × 10TB each = 10PB total data
  Files: ~1M files (typical)
  Blocks: 10PB / 128MB = ~80M blocks

Memory needed:
  Files:  1M × 200B = 200MB
  Blocks: 80M × 150B = 12GB
  Total:  ~12GB

Conclusion: A single Go process with 16GB RAM can handle ~10PB of data
at 128MB block size. Beyond that, federation is needed.
```

### Heartbeat Throughput Budget

```
Heartbeat interval: 3 seconds
1000 nodes → 333 heartbeats/second

Processing time per heartbeat (current): ~100μs
Throughput: 10,000 heartbeats/sec (one core)

Conclusion: Single-threaded heartbeat processing handles 1000 nodes easily.
The bottleneck is LOCK CONTENTION, not CPU — fix is sharding or RWMutex.
```

### Block Report Throughput Budget

```
Block report interval: 30 seconds
1000 nodes → 33 reports/second

Average blocks per DataNode: 80,000 (10TB / 128MB)
Processing time per block: ~1μs (map lookup)
Processing time per report: 80ms

At 33 reports/sec × 80ms = 2.64 seconds of processing per second
Conclusion: Block reports need ~2.64 cores of processing at 1000 nodes.
Single-threaded won't keep up — need parallel processing or batching.
```

---

## 5. Comparison with Apache Hadoop's Scaling Journey

| Hadoop Version | Max Tested Scale | Key Scaling Feature Added |
|---------------|-----------------|--------------------------|
| 0.1 (2006) | ~20 nodes | Basic MapReduce + HDFS |
| 0.20 (2009) | ~4,000 nodes | NameNode optimization, rack awareness |
| 2.0 (2012) | ~4,000 nodes | YARN (decoupled resource management) |
| 2.6 (2014) | ~10,000 nodes | NameNode Federation, HA |
| 3.0 (2017) | ~10,000+ nodes | Erasure coding, Observer NameNode |
| 3.3 (2022) | ~10,000+ nodes | RBF (Router-Based Federation) |

**mini-Hadoop today ≈ Hadoop 0.1 in terms of scaling capability** — the architecture is right (same as Hadoop 2.0+), but the optimizations from Hadoop 0.20 through 3.3 haven't been implemented yet.

The good news: we know exactly what optimizations are needed because Hadoop already discovered them. It's a matter of implementing known solutions, not inventing new ones.

---

## 6. Quick Wins: What to Do First

If you need to scale mini-Hadoop beyond 10 nodes today, start with these 4 changes (1 day of work):

```go
// 1. Replace insertion sort with stdlib sort (5 minutes)
sort.Slice(alive, func(i, j int) bool {
    return alive[i].AvailableBytes > alive[j].AvailableBytes
})

// 2. Add per-DataNode block index (1 hour)
type BlockManager struct {
    blocks   map[string]*block.Metadata
    dnBlocks map[string]map[string]bool  // NEW: nodeID → blockIDs
}

// 3. Use RWMutex for read-heavy operations (30 minutes)
// Change ProcessHeartbeat to use RLock for reads, Lock only for writes

// 4. Add metrics for monitoring scale (30 minutes)
// Track: heartbeat_processing_time, block_report_time, allocation_time
```

These 4 changes would comfortably support ~100 nodes with no architectural changes.
