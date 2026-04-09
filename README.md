# mini-Hadoop

A high-performance distributed computing framework in Go that reimplements the core architectural primitives of Apache Hadoop. Built via AI-native codebase rebuild — extracting essential features from Hadoop's 201K-node codegraph and reimplementing them in ~24K lines of Go.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          mini-Hadoop Cluster                            │
│                                                                         │
│  ┌──────────────┐  ┌──────────────────┐  ┌────────────────────────┐   │
│  │  NameNode    │  │ ResourceManager   │  │  Web Dashboards        │   │
│  │  (metadata)  │  │ (capacity sched.) │  │  :9100 (NN) :9110 (RM)│   │
│  │  + HA standby│  │ + queue mgmt      │  │  /metrics  /health     │   │
│  └──────┬───────┘  └────────┬─────────┘  └────────────────────────┘   │
│         │ gRPC              │ gRPC                                      │
│  ┌──────┴───────────────────┴──────────────────────────────────────┐   │
│  │                     Worker Nodes (N nodes)                       │   │
│  │  ┌────────────┐  ┌─────────────┐  ┌──────────────────────┐     │   │
│  │  │ DataNode   │  │ NodeManager │  │ ApplicationMaster    │     │   │
│  │  │ (blocks)   │  │ (containers)│  │ (MR or DAG engine)   │     │   │
│  │  └────────────┘  └─────────────┘  └──────────────────────┘     │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

## Features

### Core (Phase 1)
- **HDFS-lite**: Distributed block storage with 3x pipelined replication, streaming writes, heartbeat-based fault detection, automatic re-replication
- **YARN-lite**: Resource management with capacity scheduler (queue-based multi-tenancy), locality-aware container allocation
- **MapReduce**: Parallel batch processing with sort/spill, shuffle, combiner support, WordCount/SumByKey built-in jobs
- **6 gRPC services**: NameNode, DataNode, ResourceManager, NodeManager, Shuffle, common types
- **7 binaries**: namenode, datanode, resourcemanager, nodemanager, hdfs CLI, mapreduce CLI, mapworker

### Advanced (Phase 2)
- **HA NameNode**: Write-ahead edit log (WAL), leader election with pluggable backend (ZooKeeper/local), fencing tokens
- **Capacity Scheduler**: Multi-queue resource management with min/max capacity percentages, elastic bursting
- **Erasure Coding**: Reed-Solomon RS-6-3 (1.5x storage overhead vs 3x for replication, same fault tolerance)
- **Block Compression**: Gzip codec with CompressBlock/DecompressBlock (99.2% compression on repetitive text)
- **DAG Engine**: Spark-like RDD abstraction with Map, FlatMap, Filter, ReduceByKey, GroupByKey, DAG scheduler with stage splitting at shuffle boundaries
- **Raw TCP Transport**: Custom wire protocol behind BlockTransport interface for maximum block transfer throughput
- **Short-Circuit Reads**: Bypass DataNode gRPC for co-located data (direct local disk read)
- **Ack-Queue Pipeline**: Dual-queue pattern for pipeline write reliability with drain-and-retry on failure
- **Web Dashboard**: Auto-refreshing HTML UI for NameNode and ResourceManager metrics
- **Metrics Endpoints**: JSON `/metrics` and `/health` HTTP endpoints

### Security
- Command allowlist for NodeManager process execution
- Path traversal protection on block storage and container directories
- Non-root Docker containers
- Localhost-only port publishing

## Quick Start

### Prerequisites
- Go 1.23+
- Docker + Docker Compose (for cluster testing)

### Build

```bash
# Build all 7 binaries
make build

# Run all 136 unit + acceptance tests
make test

# Run with race detector
make test-race
```

### Run with Docker Compose

```bash
# Build and start 3-node cluster
make docker-build
make docker-up

# Test HDFS operations
docker compose -f docker/docker-compose.yml exec client sh -c "
  hdfs mkdir /data
  echo 'hello world' > /tmp/test.txt
  hdfs put /tmp/test.txt /data/hello.txt
  hdfs ls /data
  hdfs get /data/hello.txt /tmp/downloaded.txt
  cat /tmp/downloaded.txt
"

# Run MapReduce WordCount
docker compose -f docker/docker-compose.yml exec client sh -c "
  mapreduce --job wordcount --input /tmp/test.txt --output /tmp/wc --local
"

# View dashboards (from inside network)
docker compose -f docker/docker-compose.yml exec client sh -c "
  curl -s http://namenode:9100/metrics
  curl -s http://resourcemanager:9110/metrics
"

# Stop cluster
make docker-down
```

### Run Locally

```bash
# Terminal 1: NameNode (dashboard at localhost:9100)
./bin/namenode --port 9000

# Terminal 2: ResourceManager (dashboard at localhost:9110)
./bin/resourcemanager --port 9010

# Terminal 3: Worker
./bin/datanode --id worker-1 --port 9001 &
./bin/nodemanager --id worker-1 --port 9011

# Terminal 4: Use HDFS
./bin/hdfs mkdir /data
./bin/hdfs put ./local-file.txt /data/file.txt
./bin/hdfs ls /data
./bin/hdfs get /data/file.txt ./downloaded.txt
```

### DAG Engine (Spark-like)

```go
import "github.com/mini-hadoop/mini-hadoop/pkg/dagengine"

lines := dagengine.NewRDDFromLines(textLines, 4)
words := lines.FlatMap(splitWords)
counts := words.ReduceByKey(sumInts)
result := counts.Collect()
```

## Project Structure

```
mini-hadoop/
├── cmd/                         # 7 binary entry points
│   ├── namenode/                # NameNode server
│   ├── datanode/                # DataNode server
│   ├── resourcemanager/         # ResourceManager server
│   ├── nodemanager/             # NodeManager server
│   ├── hdfs/                    # HDFS client CLI
│   ├── mapreduce/               # MapReduce job submission CLI
│   └── mapworker/               # Map/reduce task binary
├── proto/                       # 6 gRPC service definitions
├── pkg/
│   ├── namenode/                # Namespace, block mgr, persistence, edit log, election
│   ├── datanode/                # Block storage, pipeline replication
│   ├── hdfs/                    # Client library, ack queue
│   ├── resourcemanager/         # Capacity scheduler, queues
│   ├── nodemanager/             # Container lifecycle, security
│   ├── mapreduce/               # MR engine, sort/spill, shuffle, AppMaster, combiner
│   ├── dagengine/               # Spark-like RDD + DAG scheduler
│   ├── block/                   # Block abstraction, checksums, compression, erasure coding
│   ├── config/                  # Configuration (JSON + env vars)
│   └── rpc/                     # gRPC helpers, TCP transport, metrics, dashboard
├── test/acceptance/             # 7 acceptance tests
├── docker/                      # Dockerfile + docker-compose.yml
└── doc/                         # Design docs, reports, plans
```

## Test Results

**136 tests, all passing with race detector:**

| Package | Tests | Coverage |
|---------|-------|----------|
| pkg/block | 24 | Block IDs, checksums, compression, erasure coding |
| pkg/config | 7 | Config load/save, env var overrides |
| pkg/dagengine | 12 | RDD ops, WordCount pipeline, DAG scheduler |
| pkg/datanode | 10 | Block storage, path traversal protection |
| pkg/hdfs | 6 | Ack queue lifecycle |
| pkg/mapreduce | 7 | WordCount, sort buffer, partitioner |
| pkg/namenode | 41 | Namespace, block mgr, persistence, edit log |
| pkg/nodemanager | 8 | Command allowlist, arg validation |
| pkg/resourcemanager | 16 | FIFO scheduler, queues, locality |
| test/acceptance | 7 | File I/O, replication, WordCount, shuffle |

## Manual Testing (10/10 PASS)

Verified on Windows 11 + Docker Desktop 29.3.1 with a fresh 3-node cluster. See [full sample run output](doc/manual-testing-guide_sample_run.md).

| # | Test | What it proves | Result |
|---|------|---------------|--------|
| 1 | Cluster Health | 6 services running, metrics dashboards | PASS |
| 2 | Create Directories | Namespace operations | PASS |
| 3 | Small File Upload/Download | Basic write/read pipeline | PASS |
| 4 | Large File + SHA-256 | 27MB data integrity across chunks | PASS |
| 5 | Kill Worker Node | Fault tolerance — file readable after death | PASS |
| 6 | WordCount MapReduce | Map → sort → reduce pipeline (23 words) | PASS |
| 7 | Distributed Mapworker | 2-reducer partitioned shuffle (6000 words) | PASS |
| 8 | File Operations | Delete, list, info commands | PASS |
| 9 | Metrics Dashboard | JSON /metrics + /health endpoints | PASS |
| 10 | SumByKey Job | Generic MapReduce (electronics=650) | PASS |

To run the tests yourself, follow [doc/manual-testing-guide.md](doc/manual-testing-guide.md).

## Run on Multiple Machines

mini-Hadoop can run across real separate machines (not just Docker on one host).

### Prerequisites
- All machines can reach each other over the network (TCP ports 9000-9011)
- Go 1.23+ installed on all machines, OR copy pre-built binaries
- Shared config: all workers know the NameNode and ResourceManager addresses

### Step 1: Build binaries

```bash
# On your build machine
make build
# Produces: bin/namenode, bin/datanode, bin/resourcemanager, bin/nodemanager, bin/hdfs, bin/mapreduce, bin/mapworker
```

Copy `bin/*` to all machines, or build on each machine.

### Step 2: Start master services

On **Machine A** (master node):
```bash
# Set hostname so workers can reach this machine
export MINIHADOOP_HOSTNAME=machine-a.local

# Start NameNode (port 9000) + dashboard (port 9100)
./bin/namenode --port 9000 &

# Start ResourceManager (port 9010) + dashboard (port 9110)
./bin/resourcemanager --port 9010 &

echo "Master services running on machine-a.local"
```

### Step 3: Start workers

On **Machine B** (worker 1):
```bash
export MINIHADOOP_HOSTNAME=machine-b.local
export MINIHADOOP_NAMENODE_HOST=machine-a.local
export MINIHADOOP_NAMENODE_PORT=9000
export MINIHADOOP_RM_HOST=machine-a.local
export MINIHADOOP_RM_PORT=9010

# Start DataNode + NodeManager
./bin/datanode --id worker-1 --port 9001 &
./bin/nodemanager --id worker-1 --port 9011 &
```

On **Machine C** (worker 2):
```bash
export MINIHADOOP_HOSTNAME=machine-c.local
export MINIHADOOP_NAMENODE_HOST=machine-a.local
export MINIHADOOP_NAMENODE_PORT=9000
export MINIHADOOP_RM_HOST=machine-a.local
export MINIHADOOP_RM_PORT=9010

./bin/datanode --id worker-2 --port 9001 &
./bin/nodemanager --id worker-2 --port 9011 &
```

On **Machine D** (worker 3):
```bash
export MINIHADOOP_HOSTNAME=machine-d.local
export MINIHADOOP_NAMENODE_HOST=machine-a.local
export MINIHADOOP_NAMENODE_PORT=9000
export MINIHADOOP_RM_HOST=machine-a.local
export MINIHADOOP_RM_PORT=9010

./bin/datanode --id worker-3 --port 9001 &
./bin/nodemanager --id worker-3 --port 9011 &
```

### Step 4: Use the cluster

From **any machine** with network access to the master:
```bash
export MINIHADOOP_NAMENODE_HOST=machine-a.local
export MINIHADOOP_RM_HOST=machine-a.local

# HDFS operations
./bin/hdfs mkdir /data
./bin/hdfs put ./local-file.txt /data/file.txt
./bin/hdfs get /data/file.txt ./downloaded.txt

# MapReduce
./bin/mapreduce --job wordcount --input ./input.txt --output ./output --local

# Check dashboards
curl http://machine-a.local:9100/metrics   # NameNode
curl http://machine-a.local:9110/metrics   # ResourceManager
```

### Network Topology

```
Machine A (master)          Machine B (worker-1)
┌──────────────────┐       ┌──────────────────┐
│ NameNode :9000   │◄─────►│ DataNode :9001   │
│ RM       :9010   │◄─────►│ NodeManager:9011 │
│ Dashboard:9100   │       └──────────────────┘
│ Dashboard:9110   │
└──────────────────┘       Machine C (worker-2)
        ▲                  ┌──────────────────┐
        ├─────────────────►│ DataNode :9001   │
        │                  │ NodeManager:9011 │
        │                  └──────────────────┘
        │
        │                  Machine D (worker-3)
        │                  ┌──────────────────┐
        └─────────────────►│ DataNode :9001   │
                           │ NodeManager:9011 │
                           └──────────────────┘
```

### Tips for Multi-Machine Deployment
- **Firewall:** Ensure ports 9000-9011 and 9100-9110 are open between all machines
- **DNS/Hosts:** Each machine must be resolvable by hostname. Use `/etc/hosts` if no DNS
- **Data directories:** Each worker stores blocks at `MINIHADOOP_DATA_DIR` (default `/tmp/minihadoop/datanode`). Use a dedicated disk for production
- **Scaling:** Add more workers by repeating Step 3 on additional machines with unique `--id` values

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `MINIHADOOP_NAMENODE_HOST` | localhost | NameNode hostname |
| `MINIHADOOP_NAMENODE_PORT` | 9000 | NameNode gRPC port |
| `MINIHADOOP_RM_HOST` | localhost | ResourceManager hostname |
| `MINIHADOOP_RM_PORT` | 9010 | ResourceManager gRPC port |
| `MINIHADOOP_HOSTNAME` | os.Hostname() | Service registration hostname |
| `MINIHADOOP_DATA_DIR` | /tmp/minihadoop/datanode | Block storage directory |
| `MINIHADOOP_METADATA_DIR` | /tmp/minihadoop/namenode | NameNode state directory |
| `MINIHADOOP_TEMP_DIR` | /tmp/minihadoop/temp | Temporary files |

## Benchmarking (mini-Hadoop vs Apache Hadoop)

18-benchmark comparison suite running both systems in Docker with identical configurations.

```bash
# Run all 18 benchmarks (~20-30 min)
./scripts/benchmark.sh

# Quick run (10MB, fewer iterations, ~10 min)
./scripts/benchmark.sh quick

# mini-Hadoop only (~5 min)
./scripts/benchmark.sh mini-only
```

| Category | Benchmarks | Key Metrics |
|----------|-----------|-------------|
| Storage (S1-S4) | Write/read throughput, small files, concurrent integrity | MB/s, files/s, SHA-256 |
| Fault Tolerance (F1-F4) | Detection time, durability, recovery, multi-failure | Seconds, pass/fail |
| MapReduce (M1-M4) | WordCount, SumByKey, Sort, scaling | Wall-clock ms |
| Resources (R1-R5) | Image size, memory, peak memory, startup, LOC | MB, seconds |
| Correctness (C1-C3) | Output equivalence, edge cases, replication | Match/differ |

Results saved to `benchmark_results_*/` with per-test files and summary. See [benchmark plan](doc/benchmark-plan.md) for full methodology.

## Documentation

- **[Manual Testing Guide](doc/manual-testing-guide.md)** — 10 step-by-step tests for verifying all features
- **[Sample Run Output](doc/manual-testing-guide_sample_run.md)** — Verified test output (10/10 PASS)
- **[Comprehensive Technical Report](doc/mini-hadoop-comprehensive-report.md)** — Full architecture analysis, data flow diagrams, algorithms
- **[Benchmark Plan](doc/benchmark-plan.md)** — 18 benchmarks: mini-Hadoop vs Apache Hadoop
- **[Scaling Analysis](doc/scaling-analysis.md)** — Bottleneck analysis + roadmap (3 → 1000+ nodes)
- **[Integration Plan](doc/integration-plan.md)** — How to wire library-only features into servers
- **[Phase 2 Implementation Plan](doc/phase2-plan.md)** — Detailed specs for advanced features

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Language | Go | Near-Java performance + AI-friendly + goroutines |
| Communication | gRPC + protobuf | Strongly typed, efficient, natural for Go |
| Block transport | gRPC streaming + raw TCP option | BlockTransport interface allows swapping |
| Replication | 3x pipelined + RS-6-3 erasure coding | Standard + storage-efficient option |
| Scheduler | Capacity (queue-based) | Multi-tenant with elastic bursting |
| Processing | MapReduce + DAG engine | Batch + Spark-like in-memory |
| HA | Edit log WAL + leader election | Pluggable backend (ZooKeeper/local) |
