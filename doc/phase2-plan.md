# mini-Hadoop Phase 2: Advanced Features Implementation Plan

**Date:** 2026-04-09
**Base:** PR #1 (`f56fe8e`) — 11 commits, 98 tests, Docker cluster verified
**Scope:** 6 remaining items from the roadmap (D1, D3, D4, B2, C3, C5)

---

## Executive Summary

Phase 1 delivered a working distributed computing framework with HDFS-lite, YARN-lite, MapReduce, capacity scheduling, and a web dashboard. Phase 2 adds enterprise-grade reliability, storage efficiency, and next-generation processing capabilities.

```
Phase 2 Roadmap
═══════════════

Priority 1 (Reliability)          Priority 2 (Efficiency)        Priority 3 (Next-Gen)
┌─────────────────────┐           ┌─────────────────────┐       ┌──────────────────────┐
│ B2: Ack-Queue       │           │ C3: Block           │       │ D4: Spark-like DAG   │
│     Pipeline        │           │     Compression     │       │     Engine           │
│                     │           │                     │       │                      │
│ D1: HA NameNode     │           │ C5: Raw TCP         │       │                      │
│     (ZooKeeper)     │           │     Transport       │       │                      │
│                     │           │                     │       │                      │
│                     │           │ D3: Erasure Coding   │       │                      │
└─────────────────────┘           └─────────────────────┘       └──────────────────────┘
      ~2 weeks                         ~2 weeks                      ~3-4 weeks
```

---

## B2: Ack-Queue Pipeline with Mid-Write Failure Recovery

### Goal
Replace the fire-and-forget pipeline write with Hadoop's dual-queue pattern, enabling detection and recovery from DataNode failures that occur during a block write — not just after.

### Current State
- DataNode WriteBlock forwards chunks to the next pipeline node
- On forward failure: `nextStream = nil`, local write continues
- Client receives success but downstream replicas may be incomplete
- NameNode detects under-replication only after heartbeat timeout (10s)

### Target State
- Client maintains **data queue** (chunks to send) and **ack queue** (sent, awaiting acknowledgment)
- Ack flows backward through the pipeline: DN3 → DN2 → DN1 → Client
- On mid-pipeline failure:
  1. Halt pipeline immediately
  2. Drain ack queue back to data queue (unacknowledged packets)
  3. Request new pipeline from NameNode (with new generation stamp)
  4. Resume write to surviving nodes + new replacement node
  5. Failed node's partial block is orphaned by generation stamp mismatch

### Architecture

```
Normal Pipeline Write:
  Client ──chunk[0]──> DN1 ──chunk[0]──> DN2 ──chunk[0]──> DN3
  Client <──ack[0]─── DN1 <──ack[0]─── DN2 <──ack[0]─── DN3

Mid-Write Failure Recovery:
  Client ──chunk[5]──> DN1 ──chunk[5]──> DN2 ──╳ (DN2 dies)
                                                │
  1. DN1 detects send error, signals Client     │
  2. Client: drain ack queue → data queue       │
     [chunk[3], chunk[4], chunk[5]] unsent      │
  3. Client → NameNode: GetNewPipeline(block, genStamp+1)
  4. NameNode returns [DN1, DN3] (skip dead DN2)
  5. Client resumes: chunk[3] → DN1 → DN3
  6. DN2's partial block has old genStamp → orphaned on recovery
```

### Implementation Plan

#### B2.1: Ack Queue Data Structures
**File:** `pkg/hdfs/ackqueue.go` (new)

```go
type AckQueue struct {
    mu       sync.Mutex
    pending  []Packet       // Sent but not yet acknowledged
    data     []Packet       // Waiting to be sent (or drained back from ack)
    onAck    func(seqNo int) // Callback when packet is acknowledged
}

type Packet struct {
    SeqNo  int
    Data   []byte
    Offset int64
}
```

Key operations:
- `Enqueue(packet)` → add to data queue
- `Dequeue()` → move from data queue to ack queue, return packet for sending
- `Acknowledge(seqNo)` → remove from ack queue (confirmed by all downstream)
- `DrainToDataQueue()` → on failure, move all ack queue packets back to front of data queue
- `Reset()` → clear both queues (for pipeline rebuild)

#### B2.2: Pipeline Manager
**File:** `pkg/hdfs/pipeline.go` (new)

```go
type Pipeline struct {
    blockID     string
    genStamp    int64
    nodes       []string        // Current pipeline node addresses
    ackQueue    *AckQueue
    nnClient    pb.NameNodeServiceClient
    streams     []pb.DataNodeService_WriteBlockClient
    maxRetries  int
}
```

Key operations:
- `NewPipeline(blockID, nodes, nnClient)` → create pipeline with ack tracking
- `SendPacket(data)` → enqueue, send through pipeline, wait for ack
- `HandleFailure(failedNode)`:
  1. Close all streams
  2. `ackQueue.DrainToDataQueue()`
  3. Call NameNode `RebuildPipeline(blockID, genStamp+1, excludeNodes=[failedNode])`
  4. Open new streams to rebuilt pipeline
  5. Resend all packets in data queue
- `Close()` → send final packet, wait for all acks, close streams

#### B2.3: Proto Changes
**File:** `proto/namenode.proto`

Add new RPC:
```protobuf
rpc RebuildPipeline(RebuildPipelineRequest) returns (RebuildPipelineResponse);

message RebuildPipelineRequest {
    string block_id = 1;
    int64 new_generation_stamp = 2;
    repeated string exclude_nodes = 3;  // Nodes to exclude from new pipeline
}

message RebuildPipelineResponse {
    repeated string pipeline = 1;
    int64 generation_stamp = 2;
    string error = 3;
}
```

#### B2.4: DataNode Ack Propagation
**File:** `pkg/datanode/server.go`

Modify WriteBlock:
- After successfully writing a chunk to local disk AND forwarding to next node:
  - Wait for downstream ack
  - Propagate ack backward to upstream caller
- On downstream failure:
  - Return error to upstream immediately
  - Let client handle pipeline rebuild

#### B2.5: Tests
- Unit test: AckQueue drain/enqueue/acknowledge operations
- Unit test: Pipeline rebuild with mock NameNode
- Integration test: kill middle DataNode during 10MB write → verify file completes correctly
- AC-2 enhancement: verify ack-queue recovery, not just heartbeat-based re-replication

### Effort Estimate
- B2.1-B2.2: 2 days (new files, ~400 lines)
- B2.3-B2.4: 1 day (proto change + DataNode modification)
- B2.5: 1 day (tests)
- **Total: ~4 days**

### Risks
- gRPC bidirectional streaming is needed for ack propagation — current WriteBlock uses client-streaming only
- Alternative: use a separate ack channel (simpler but less faithful to Hadoop)
- Pipeline rebuild requires NameNode to track which blocks are being written (partial state)

---

## D1: HA NameNode with ZooKeeper Election

### Goal
Eliminate the single point of failure (NameNode). When the active NameNode dies, a standby automatically takes over within 30 seconds with no data loss.

### Architecture

```
┌────────────────────┐       ┌────────────────────┐
│  Active NameNode   │       │ Standby NameNode   │
│  (serves RPCs)     │       │ (replays edit log) │
│                    │       │                    │
│  ┌──────────────┐  │       │  ┌──────────────┐  │
│  │ Namespace    │──┼───────┼─>│ Namespace    │  │
│  │ (in-memory)  │  │ edit  │  │ (in-memory)  │  │
│  └──────────────┘  │ log   │  └──────────────┘  │
│  ┌──────────────┐  │ sync  │  ┌──────────────┐  │
│  │ Block Map    │  │       │  │ Block Map    │  │
│  │              │  │       │  │ (from DN     │  │
│  └──────────────┘  │       │  │  reports)    │  │
└────────┬───────────┘       └────────┬───────────┘
         │                            │
         │     ┌──────────────┐       │
         └────>│  ZooKeeper   │<──────┘
               │  (election)  │
               │              │
               │  /leader     │ ← ephemeral znode
               │  /fence      │ ← fencing token
               └──────────────┘
                     ▲
                     │
            ┌────────────────┐
            │ ZKFC (Failover │
            │  Controller)   │
            │ one per NN     │
            └────────────────┘
```

### Implementation Plan

#### D1.1: Edit Log (Write-Ahead Log)
**File:** `pkg/namenode/editlog.go` (new, ~200 lines)

Replace the periodic JSON dump with a proper write-ahead log:
```go
type EditLog struct {
    file     *os.File
    encoder  *json.Encoder
    sequence int64
}

type EditEntry struct {
    Sequence  int64      `json:"seq"`
    Timestamp time.Time  `json:"ts"`
    Operation string     `json:"op"`    // CREATE_FILE, COMPLETE_FILE, DELETE, MKDIR, ADD_BLOCK
    Path      string     `json:"path"`
    Data      json.RawMessage `json:"data"` // Operation-specific payload
}
```

Operations:
- Every namespace mutation → append to edit log before returning success
- `Replay(entries)` → rebuild namespace from edit log (for standby startup)
- `Checkpoint()` → snapshot full namespace + truncate edit log

#### D1.2: Edit Log Sync (Active → Standby)
**File:** `pkg/namenode/editlogsync.go` (new, ~150 lines)

Options (pick one):
- **Option A: Shared filesystem** — both NNs read/write edit log to a shared NFS mount
- **Option B: gRPC streaming** — Active streams edit entries to Standby in real-time
- **Option C: JournalNode quorum** — like real Hadoop (3 journal nodes, majority write)

**Recommended: Option B** (simplest for mini-Hadoop):
```protobuf
service NameNodeHAService {
    rpc StreamEditLog(StreamEditLogRequest) returns (stream EditEntry);
    rpc GetNamespaceSnapshot(SnapshotRequest) returns (SnapshotResponse);
}
```

Standby connects to Active, requests edit log from last known sequence, and replays entries as they arrive. On failover, Standby has near-real-time namespace state.

#### D1.3: ZooKeeper Integration
**File:** `pkg/namenode/election.go` (new, ~200 lines)

Use `go-zookeeper/zk` library:
```go
type LeaderElection struct {
    zkConn    *zk.Conn
    zkPath    string     // /minihadoop/leader
    nodeID    string     // This NameNode's ID
    isLeader  bool
    onElected func()     // Callback when this node becomes leader
    onLost    func()     // Callback when leadership is lost
}
```

Election protocol:
1. Each NameNode creates ephemeral sequential znode at `/minihadoop/leader/n_`
2. Lowest sequence number is the leader
3. Non-leaders watch the next-lower znode (herd-avoidance)
4. When leader's session expires, its znode disappears → next node becomes leader
5. New leader calls `onElected()` → start serving RPCs + accept DataNode heartbeats

#### D1.4: Fencing
**File:** Part of `pkg/namenode/election.go`

Prevent split-brain:
- Active NameNode writes a fencing token to ZooKeeper on election
- Before serving any write RPC, check fencing token matches own session
- On failover, new leader updates fencing token, old leader's writes are rejected
- DataNodes: on heartbeat, check fencing token in response → re-register with new leader if changed

#### D1.5: ZKFC (ZooKeeper Failover Controller)
**File:** `cmd/zkfc/main.go` (new binary, ~100 lines)

Lightweight process that:
1. Monitors local NameNode health (gRPC health check)
2. Participates in ZooKeeper election on behalf of NameNode
3. On local NN failure → release ZK session → triggers failover
4. On becoming leader → tell local NN to activate

#### D1.6: Client-Side Failover
**File:** `pkg/hdfs/client.go` (modify)

HDFS client maintains addresses for both Active and Standby NameNode:
```go
type Client struct {
    activeNN   string
    standbyNN  string
    currentNN  *grpc.ClientConn
}
```

On RPC failure:
1. Try standby NameNode
2. If standby responds as active → switch to it
3. If neither responds → return error

#### D1.7: Tests
- Unit: EditLog append + replay
- Unit: LeaderElection with embedded ZK test server
- Integration: start 2 NameNodes + ZK → kill active → verify standby takes over within 30s
- Integration: verify DataNodes re-register with new leader
- Integration: verify client failover is transparent

### Dependencies
- External: `github.com/go-zookeeper/zk` (well-maintained ZK client for Go)
- Infrastructure: ZooKeeper cluster (3 nodes for quorum, or single node for dev)
- Docker: add ZK service to docker-compose.yml

### Effort Estimate
- D1.1: 1 day (edit log)
- D1.2: 1 day (edit log sync)
- D1.3: 2 days (ZK integration + election)
- D1.4: 0.5 days (fencing)
- D1.5: 0.5 days (ZKFC binary)
- D1.6: 0.5 days (client failover)
- D1.7: 1.5 days (tests)
- **Total: ~7 days**

### Risks
- ZooKeeper adds operational complexity (another service to manage)
- Edit log sync latency determines RPO (Recovery Point Objective)
- Split-brain prevention with fencing is subtle — must be tested thoroughly

---

## D3: Erasure Coding (6+3 Reed-Solomon)

### Goal
Replace 3x replication with erasure coding for storage-efficient fault tolerance: 1.5x overhead (vs 3x) with the same level of fault tolerance (survives any 3 node failures).

### Architecture

```
3x Replication (current):
  Block → [Copy1] [Copy2] [Copy3]
  Storage: 3x original size
  Tolerance: 2 node failures

Erasure Coding (6+3 RS):
  Block → split into 6 data chunks + compute 3 parity chunks
  [D1] [D2] [D3] [D4] [D5] [D6] [P1] [P2] [P3]
  Storage: 1.5x original size (9 chunks for 6 chunks of data)
  Tolerance: any 3 chunk failures (same as 3x replication)
```

### Implementation Plan

#### D3.1: Reed-Solomon Codec
**File:** `pkg/block/erasure.go` (new, ~300 lines)

Use `github.com/klauspost/reedsolomon` library:
```go
type ErasureCodec struct {
    dataShards   int // 6
    parityShards int // 3
    encoder      reedsolomon.Encoder
}

func NewErasureCodec(data, parity int) *ErasureCodec
func (ec *ErasureCodec) Encode(data []byte) ([][]byte, error)      // → 9 shards
func (ec *ErasureCodec) Decode(shards [][]byte) ([]byte, error)    // reconstruct from any 6 of 9
func (ec *ErasureCodec) Verify(shards [][]byte) (bool, error)      // check parity
```

#### D3.2: Block Group
**File:** `pkg/block/blockgroup.go` (new, ~150 lines)

```go
type BlockGroup struct {
    GroupID      string
    DataBlocks   []BlockGroupEntry  // 6 data chunks
    ParityBlocks []BlockGroupEntry  // 3 parity chunks
    OriginalSize int64
    Policy       string             // "RS-6-3"
}

type BlockGroupEntry struct {
    BlockID  string
    NodeAddr string
    Index    int    // 0-8 (0-5 data, 6-8 parity)
}
```

#### D3.3: NameNode EC Policy Support
**File:** Modify `pkg/namenode/blockmanager.go`

- Add `ECPolicy` field to file metadata (per-file or per-directory)
- `AllocateBlockGroup()` — allocate 9 blocks across 9 different nodes
- Block placement: spread chunks across maximum number of distinct nodes
- Under-replication detection: < 6 surviving chunks → reconstruct

#### D3.4: DataNode EC Support
- DataNode stores EC chunks like regular blocks (no change to storage)
- Reconstruction: NameNode instructs healthy DataNodes to read 6 chunks, compute missing, write to replacement node

#### D3.5: Client EC Integration
**File:** Modify `pkg/hdfs/client.go`

Write path:
1. Buffer full block (128MB)
2. Encode with RS codec → 9 shards
3. Write each shard to a different DataNode (parallel)

Read path:
1. Get block group locations from NameNode
2. Read any 6 shards (prefer local, then nearest)
3. Decode → original block data
4. If < 6 available → degraded-mode read (read all available + reconstruct)

#### D3.6: Tests
- Unit: RS encode/decode correctness (random data)
- Unit: RS decode with 1, 2, 3 missing shards
- Integration: write EC file → kill 3 DataNodes → read succeeds
- Benchmark: storage comparison (3x replication vs RS-6-3)

### Dependencies
- External: `github.com/klauspost/reedsolomon` (high-performance RS in Go, SIMD-optimized)

### Effort Estimate
- D3.1: 1 day (codec wrapper)
- D3.2: 0.5 days (block group)
- D3.3: 2 days (NameNode EC policy + allocation)
- D3.4: 0.5 days (DataNode — minimal changes)
- D3.5: 2 days (client EC read/write)
- D3.6: 1 day (tests)
- **Total: ~7 days**

---

## C3: Block Compression

### Goal
Transparent compression for stored blocks to reduce storage footprint and improve I/O throughput (compressed data = less disk I/O).

### Implementation Plan

#### C3.1: Compression Codec Interface
**File:** `pkg/block/compress.go` (new, ~150 lines)

```go
type CompressionCodec interface {
    Name() string
    Compress(src []byte) ([]byte, error)
    Decompress(src []byte) ([]byte, error)
}

// Built-in codecs
type SnappyCodec struct{}   // Fast, moderate ratio (~2-3x)
type ZstdCodec struct{}     // Slower, better ratio (~3-5x)
type NoneCodec struct{}     // Pass-through (no compression)
```

Dependencies: `github.com/golang/snappy`, `github.com/klauspost/compress/zstd`

#### C3.2: DataNode Integration
- On WriteBlock: compress data before writing to disk
- On ReadBlock: decompress before streaming to client
- Block metadata tracks compression codec used
- Backward compatible: uncompressed blocks read as-is

#### C3.3: Configuration
- Per-file compression codec in CreateFile request
- Default codec configurable (snappy recommended)
- `hdfs put --compression snappy` CLI flag

#### C3.4: Tests
- Unit: compress/decompress round-trip for each codec
- Integration: write compressed file → read back → verify checksum
- Benchmark: throughput and storage savings

### Effort Estimate: ~3 days

---

## C5: Raw TCP Transport

### Goal
Replace gRPC streaming with raw TCP for block data transfer, maximizing throughput for 128MB block writes/reads.

### Implementation Plan

#### C5.1: TCP BlockTransport Implementation
**File:** `pkg/rpc/tcptransport.go` (new, ~250 lines)

Implement the `BlockTransport` interface defined in Phase 1:
```go
type TCPTransport struct {
    pool *ConnPool  // Connection pool per DataNode address
}

func (t *TCPTransport) SendBlock(blockID string, data io.Reader, size int64, pipeline []string) error
func (t *TCPTransport) ReceiveBlock(blockID string, sourceAddr string) (io.ReadCloser, int64, error)
func (t *TCPTransport) Close() error
```

Wire protocol:
```
Frame: [magic:4][version:1][type:1][blockID_len:2][blockID:N][payload_len:4][payload:M][checksum:4]
Types: WRITE_HEADER=1, DATA_CHUNK=2, ACK=3, ERROR=4
```

#### C5.2: Connection Pool
```go
type ConnPool struct {
    mu    sync.Mutex
    conns map[string][]*net.TCPConn  // addr → idle connections
    maxIdle int
}
```

#### C5.3: DataNode TCP Server
- Listen on a separate TCP port (default: 9002)
- Handle WRITE and READ requests
- Pipeline forwarding over TCP (not gRPC)

#### C5.4: Configuration Switch
```go
// In config:
BlockTransportType string `json:"block_transport"` // "grpc" or "tcp"
```

Both transports implement `BlockTransport` interface — swap without changing callers.

#### C5.5: Tests + Benchmark
- Integration: write/read via TCP transport
- Benchmark: gRPC vs TCP throughput on 128MB blocks (target: >2x improvement)

### Effort Estimate: ~5 days

---

## D4: Spark-like In-Memory DAG Engine

### Goal
Add a second processing engine on YARN-lite that supports multi-stage directed acyclic graph (DAG) execution with in-memory intermediate data — conceptually similar to Apache Spark.

### Architecture

```
MapReduce:  Map ──disk──> Shuffle ──disk──> Reduce
DAG Engine: Stage1 ──memory──> Stage2 ──memory──> Stage3 ──disk──> Output

Key difference: intermediate data stays in memory between stages,
avoiding the disk I/O bottleneck that limits MapReduce performance.
```

### Implementation Plan

#### D4.1: RDD-like Abstraction
**File:** `pkg/dagengine/rdd.go` (new, ~300 lines)

```go
type RDD struct {
    id         string
    partitions []Partition
    deps       []Dependency   // Parent RDDs
    compute    func(partition Partition) Iterator
    cached     bool
}

type Partition struct {
    Index int
    Data  []KeyValue  // In-memory partition data
}

type Dependency struct {
    Type   string  // "narrow" (1:1) or "wide" (shuffle)
    Parent *RDD
}
```

Operations:
- `Map(fn)` → narrow dependency (same partition count)
- `Filter(fn)` → narrow dependency
- `ReduceByKey(fn)` → wide dependency (requires shuffle)
- `GroupByKey()` → wide dependency
- `Join(other)` → wide dependency
- `Collect()` → materialize to driver
- `SaveToHDFS(path)` → write partitions to HDFS

#### D4.2: DAG Scheduler
**File:** `pkg/dagengine/scheduler.go` (new, ~400 lines)

```go
type DAGScheduler struct {
    stages    []*Stage
    rmClient  pb.ResourceManagerServiceClient
}

type Stage struct {
    id         int
    rdds       []*RDD
    deps       []*Stage      // Parent stages that must complete first
    shuffleDep bool          // Does this stage require a shuffle boundary?
    tasks      []*Task
    state      string        // PENDING, RUNNING, COMPLETED, FAILED
}
```

Scheduling:
1. Walk RDD lineage backward to find shuffle boundaries
2. Split into stages at each wide dependency
3. Execute stages in topological order (parallelize within stages)
4. Narrow dependencies → pipeline within a single task
5. Wide dependencies → write shuffle output, start new stage

#### D4.3: In-Memory Shuffle
**File:** `pkg/dagengine/shuffle.go` (new, ~200 lines)

Unlike MapReduce disk-based shuffle:
- Shuffle data stays in memory (hash-partitioned across executors)
- Use gRPC streaming to transfer partitions between stages
- Spill to disk only when memory pressure exceeds threshold

#### D4.4: Executor
**File:** `pkg/dagengine/executor.go` (new, ~200 lines)

Runs as a YARN-lite container:
- Receives task assignment from DAG scheduler
- Executes RDD compute chain on assigned partition
- Caches intermediate results in memory
- Serves shuffle data to downstream stages

#### D4.5: Driver Program Interface
**File:** `pkg/dagengine/context.go` (new, ~150 lines)

```go
type SparkContext struct {
    appName  string
    rmClient pb.ResourceManagerServiceClient
    nnClient pb.NameNodeServiceClient
}

// Example usage:
ctx := dagengine.NewSparkContext("wordcount", rmAddr, nnAddr)
lines := ctx.TextFile("/input/data.txt")           // RDD from HDFS
words := lines.FlatMap(splitWords)                   // Narrow
pairs := words.Map(func(w string) (string, int) { return w, 1 })  // Narrow
counts := pairs.ReduceByKey(func(a, b int) int { return a + b })  // Wide (shuffle)
counts.SaveToHDFS("/output/wordcount")               // Action (triggers execution)
```

#### D4.6: Tests
- Unit: RDD lineage construction and stage splitting
- Unit: DAG scheduler topological sort
- Integration: WordCount via DAG engine vs MapReduce — verify same output
- Benchmark: DAG engine vs MapReduce throughput (expect 2-5x improvement for multi-stage jobs)

### Effort Estimate
- D4.1: 2 days (RDD abstraction)
- D4.2: 3 days (DAG scheduler — most complex)
- D4.3: 2 days (in-memory shuffle)
- D4.4: 1 day (executor)
- D4.5: 1 day (driver interface)
- D4.6: 2 days (tests + benchmark)
- **Total: ~11 days**

### Risks
- Memory management for in-memory shuffle is complex (need spill-to-disk fallback)
- DAG scheduler stage splitting at shuffle boundaries requires correct lineage tracking
- Go's garbage collector may cause latency spikes with large in-memory datasets

---

## Execution Timeline

```
Week 1-2:  B2 (Ack-Queue Pipeline) ─────────────────── 4 days
           C3 (Block Compression) ───────────────────── 3 days

Week 3-4:  D1 (HA NameNode) ────────────────────────── 7 days

Week 5-6:  C5 (Raw TCP Transport) ──────────────────── 5 days
           D3 (Erasure Coding) ─────────────────────── 7 days

Week 7-9:  D4 (Spark-like DAG Engine) ──────────────── 11 days

Total:     ~37 engineering days (~8-9 weeks)
```

## Priority Order

| # | Item | Impact | Effort | Why This Order |
|---|------|--------|--------|----------------|
| 1 | B2: Ack-Queue Pipeline | High | 4 days | Completes the core HDFS write path |
| 2 | C3: Block Compression | Medium | 3 days | Quick win, immediate storage savings |
| 3 | D1: HA NameNode | Critical | 7 days | Eliminates single point of failure |
| 4 | C5: Raw TCP Transport | Medium | 5 days | Performance optimization for block I/O |
| 5 | D3: Erasure Coding | High | 7 days | 50% storage reduction |
| 6 | D4: Spark-like DAG Engine | Very High | 11 days | Next-gen processing, biggest feature |

## Success Criteria

| Item | Gate |
|------|------|
| B2 | Kill DN mid-write → pipeline recovers → file completes correctly |
| C3 | Storage reduced >50% with snappy on text data, throughput maintained |
| D1 | Kill active NN → standby takes over <30s → no data loss |
| C5 | Block write throughput >2x vs gRPC on 128MB blocks |
| D3 | 9-node cluster with RS-6-3 → kill 3 nodes → all files readable |
| D4 | WordCount via DAG engine → 2-5x faster than MapReduce on multi-stage jobs |

## Dependencies Summary

| Item | External Libraries |
|------|-------------------|
| B2 | None (pure Go) |
| C3 | `github.com/golang/snappy`, `github.com/klauspost/compress/zstd` |
| D1 | `github.com/go-zookeeper/zk` |
| C5 | None (pure Go, net/tcp) |
| D3 | `github.com/klauspost/reedsolomon` |
| D4 | None (pure Go) |
