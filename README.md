# mini-Hadoop

A minimal, high-performance distributed computing framework in Go that implements the core architectural primitives of Apache Hadoop: distributed fault-tolerant storage (HDFS-like), parallel data-local batch processing (MapReduce), and decoupled resource management (YARN-lite).

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         mini-Hadoop Cluster                         │
│                                                                     │
│  ┌──────────────┐    ┌──────────────────┐    ┌───────────────────┐ │
│  │  NameNode    │    │ ResourceManager   │    │  Client CLI       │ │
│  │  (metadata)  │    │ (FIFO scheduler)  │    │  (file ops + jobs)│ │
│  └──────┬───────┘    └────────┬─────────┘    └─────────┬─────────┘ │
│         │ gRPC                │ gRPC                   │ gRPC      │
│  ┌──────┴─────────────────────┴────────────────────────┴─────────┐ │
│  │                    Worker Nodes (N nodes)                      │ │
│  │  ┌────────────┐  ┌─────────────┐  ┌─────────────────────┐    │ │
│  │  │ DataNode   │  │ NodeManager │  │ ApplicationMaster   │    │ │
│  │  │ (blocks)   │  │ (containers)│  │ (per-job, dynamic)  │    │ │
│  │  └────────────┘  └─────────────┘  └─────────────────────┘    │ │
│  └───────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

### Three-Layer Design

1. **Storage Layer (HDFS-lite)** - NameNode manages namespace + block mapping; DataNodes store 128MB block replicas with 3x pipelined replication
2. **Resource Management (YARN-lite)** - ResourceManager with FIFO scheduler; NodeManagers launch containers as OS processes
3. **Processing (MapReduce)** - Input splitting aligned to HDFS blocks, in-memory sort/spill, shuffle across reducers, data-local task scheduling

### Communication

All inter-component communication via gRPC with Protocol Buffers (6 service definitions).

## Quick Start

### Prerequisites

- Go 1.23+
- protoc (Protocol Buffers compiler)
- protoc-gen-go, protoc-gen-go-grpc

### Build

```bash
# Generate proto code
make proto

# Build all binaries
make build

# Run unit tests
make test

# Run tests with race detector
make test-race
```

### Run Locally (single machine, multiple processes)

```bash
# Terminal 1: Start NameNode
./bin/namenode --port 9000

# Terminal 2: Start ResourceManager
./bin/resourcemanager --port 9010

# Terminal 3-5: Start workers (DataNode + NodeManager each)
./bin/datanode --id worker-1 --port 9001 &
./bin/nodemanager --id worker-1 --port 9011

# Terminal 6: Use HDFS CLI
./bin/hdfs mkdir /data
./bin/hdfs put ./local-file.txt /data/file.txt
./bin/hdfs ls /data
./bin/hdfs get /data/file.txt ./downloaded.txt
```

### Run with Docker Compose

```bash
# Build and start 3-node cluster
make docker-build
make docker-up

# Run acceptance tests
make docker-test

# Stop cluster
make docker-down
```

## Project Structure

```
mini-hadoop/
├── cmd/                    # Binary entry points
│   ├── namenode/           # NameNode server
│   ├── datanode/           # DataNode server
│   ├── resourcemanager/    # ResourceManager server
│   ├── nodemanager/        # NodeManager server
│   ├── hdfs/               # HDFS client CLI
│   └── mapreduce/          # MapReduce job submission CLI
├── proto/                  # Protocol Buffer definitions (6 services)
├── pkg/                    # Core library packages
│   ├── namenode/           # Namespace, block manager, persistence
│   ├── datanode/           # Block storage, pipeline replication
│   ├── hdfs/               # HDFS client library
│   ├── resourcemanager/    # FIFO scheduler, container allocation
│   ├── nodemanager/        # Container lifecycle (OS processes)
│   ├── mapreduce/          # MR engine: types, sort, shuffle, jobs
│   ├── block/              # Block abstraction, checksums
│   ├── config/             # Configuration management
│   └── rpc/                # gRPC helpers, BlockTransport interface
├── test/
│   └── acceptance/         # Acceptance tests (AC-1 through AC-7)
├── docker/                 # Dockerfile + docker-compose.yml
├── go.mod
└── Makefile
```

## Key Algorithms

- **Pipeline Replication**: Client streams data to DN1, which forwards to DN2, which forwards to DN3 (pipelined, not broadcast)
- **Heartbeat Monitoring**: DataNodes send heartbeats every 3s; NameNode marks dead after 10s timeout and triggers re-replication
- **Block Placement**: 3 replicas on 3 different nodes, sorted by available capacity
- **FIFO Scheduler**: Container requests allocated in order, with locality preference (prefer nodes holding input blocks)
- **Sort/Spill**: Map output buffered in memory, sorted by (partition, key), spilled to disk at 80% threshold, merged into per-partition files
- **Shuffle**: Reducers fetch their assigned partitions from all mapper nodes, merge-sort, group by key

## Acceptance Criteria

| # | Test | Status |
|---|------|--------|
| AC-1 | File Write/Read with SHA-256 verification | Tested |
| AC-2 | Block replication recovery within 30s | Docker required |
| AC-3 | WordCount correctness vs reference | PASS |
| AC-4 | Data locality >70% map tasks | Docker required |
| AC-5 | Node failure mid-job, job completes | Docker required |
| AC-6 | Shuffle correctness (1000 keys, multi-reducer) | PASS |
| AC-7 | Linear scaling within 30% of ideal | Docker required |

## Design Decisions (ADRs)

1. **Block Transport**: gRPC streaming (not raw TCP) with BlockTransport interface for future swap
2. **NameNode Crash**: Recovery is a non-goal; periodic JSON state dump only
3. **AM Crash**: Recovery is a non-goal; job marked FAILED if AM dies
4. **Container Limits**: Advisory only (no OS-level enforcement via cgroups)

## Specification

Full specification and consensus plan:
- Spec: `.omc/specs/deep-interview-mini-hadoop.md`
- Plan: `.omc/plans/mini-hadoop-consensus-plan.md`
