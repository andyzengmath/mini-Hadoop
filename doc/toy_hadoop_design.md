**Comprehensive Report: Hadoop Architecture Review and Key Features for a Toy/MVP Implementation**

Hadoop is an open-source, Java-based distributed computing framework designed for storing and processing massive datasets (petabyte-scale) on clusters of commodity hardware. It assumes hardware failures are common and handles them automatically through replication and task re-execution. Its design prioritizes high throughput, scalability, data locality (moving computation to data rather than data to computation), and fault tolerance over low-latency POSIX compliance.

The framework's core value lies in its ability to provide reliable, parallel batch processing without requiring expensive specialized hardware. For an AI-native codebase rebuild pipeline, extracting key features and dataflows from Hadoop means identifying the minimal set of primitives that deliver its signature capabilities (distributed fault-tolerant storage + parallel data-local processing) while allowing a coding agent to implement a functional "toy" version. The toy should preserve **performance characteristics** (e.g., high aggregate bandwidth via data locality, efficient I/O through large blocks and replication pipelines) but omit non-essential features (e.g., high availability, advanced security, federation, snapshots, or multi-engine support).

### 1. Overall Hadoop Architecture and Structure

Hadoop follows a **master-slave (or master-worker) topology** with three primary layers plus supporting utilities:

- **Storage Layer**: Hadoop Distributed File System (HDFS) — handles distributed, fault-tolerant storage.
- **Resource Management Layer**: YARN (Yet Another Resource Negotiator) — manages cluster resources dynamically and schedules tasks.
- **Processing Layer**: MapReduce — the original (and still core) batch processing model that runs on YARN.
- **Hadoop Common**: Shared Java libraries, utilities, RPC framework, configuration, authentication primitives, etc. (the glue for all modules).

**Codebase Structure (High-Level)**: Hadoop is modular and organized as a set of subprojects (Maven/Gradle modules in the source tree). The four foundational ones are:
- `hadoop-common`: Core utilities, I/O, RPC, security abstractions.
- `hadoop-hdfs`: HDFS-specific code (NameNode, DataNode, client protocols).
- `hadoop-yarn`: Resource management (ResourceManager, NodeManager, ApplicationMaster framework).
- `hadoop-mapreduce-client`: MapReduce implementation (including MRAppMaster for YARN integration).

Additional modules exist for tools, tests, and ecosystem projects, but the core is these four. The architecture evolved from Hadoop 1.x (monolithic JobTracker/TaskTracker) to Hadoop 2.x+ (YARN decoupling resources from processing). A toy implementation should target the modern (YARN-based) design for better scalability and generality, but can simplify the resource layer significantly.

Dataflow overview: User data lands in HDFS (split into blocks, replicated). A job (e.g., MapReduce) is submitted to YARN. The framework launches tasks in containers on nodes where data resides (data locality), processes in parallel, shuffles intermediate results, and writes final output back to HDFS.

### 2. Detailed Component Breakdown

#### HDFS (Storage Layer) — The Foundation
HDFS is a **master/slave architecture** optimized for **write-once-read-many** workloads and streaming access to large files.

- **Key Concepts**:
  - Files are split into large **blocks** (default 128 MB; last block may be smaller). Blocks are the unit of storage and replication.
  - **Replication factor** (default 3): Each block has multiple copies for fault tolerance.
  - **Rack awareness**: Replicas are placed intelligently (e.g., one on the writer's node/rack, one on a different rack, one elsewhere) to balance performance and reliability.

- **NameNode (Master)**: Single point of metadata management.
  - Maintains the filesystem namespace (directory tree, file-to-block mapping, permissions, replication factors).
  - Stores metadata persistently in FsImage (snapshot) + EditLog (transaction log); checkpoints periodically for consistency.
  - Coordinates block placement, replication, and re-replication on failures.
  - **No user data flows through it** — only metadata.

- **DataNodes (Slaves)**: One per worker node.
  - Store actual blocks as local files.
  - Handle read/write requests from clients.
  - Send **heartbeats** (liveness) and **block reports** (list of blocks) to NameNode periodically.
  - Perform block creation/deletion/replication as instructed by NameNode.

- **Read/Write Data Flows** (Critical for performance):
  - **Write**: Client asks NameNode for block locations → pipelines data through DataNodes (first stores + forwards to next in pipeline) → pipeline ensures replication.
  - **Read**: Client asks NameNode for locations → reads from **closest replica** (same rack/node preferred) for bandwidth efficiency (data locality).

- **Fault Tolerance & Reliability**:
  - Automatic re-replication if replicas drop below target.
  - Checksums for data integrity (detect corruption → fetch from another replica).
  - Heartbeat timeout → mark DataNode dead → re-replicate its blocks.
  - Safemode on startup until enough block reports are received.

**Essential for Toy**: Block splitting, replication (fixed factor 3), basic NameNode metadata (in-memory + simple persistence), DataNode storage/heartbeats, pipeline writes, data-local reads, and re-replication on simulated failures. Omit: full HA NameNode, snapshots, quotas, storage policies, federation.

#### YARN (Resource Management Layer)
YARN decouples **resource management** (global) from **job/application scheduling** (per-job). This replaced the monolithic JobTracker and enables multi-tenancy and support for non-MapReduce workloads.

- **Core Components**:
  - **ResourceManager (RM)**: Master daemon. Contains Scheduler (allocates resources) + ApplicationsManager (handles submissions, launches first ApplicationMaster).
  - **NodeManager (NM)**: Per-node slave. Launches/manages containers, monitors resource usage (CPU/memory/etc.), reports to RM.
  - **ApplicationMaster (AM)**: Per-application (framework-specific). Negotiates additional containers from RM and coordinates task execution with NMs.
  - **Container**: Logical bundle of resources (memory, CPU, etc.) on a node.

- **Workflow**:
  1. Client submits application to RM.
  2. RM launches AM in first container.
  3. AM requests more containers from RM (based on needs, e.g., for map/reduce tasks).
  4. RM's Scheduler allocates containers (pluggable policies: capacity/fair).
  5. AM instructs NMs to launch tasks in those containers.
  6. Tasks report progress/failures back to AM.

- **Key Features**: Dynamic resource allocation, multi-tenancy via queues, scalability (via federation in full Hadoop).

**Essential for Toy**: Basic RM (simple scheduler, e.g., FIFO or round-robin), NMs for container management, per-job AM for task coordination. Data locality hints (request containers on nodes with HDFS blocks) are crucial for performance. Simplify: No pluggable schedulers, no HA/federation, no advanced reservations or timeline service.

#### MapReduce (Processing Layer) — The Original Workload
MapReduce is a functional programming model for parallel batch processing that runs on YARN (MRAppMaster acts as the AM).

- **Programming Model**:
  - Input: `<key, value>` pairs (via InputFormat/RecordReader).
  - **Map**: User-defined `map(key1, value1) → list(key2, value2)`.
  - **Shuffle/Sort**: Framework partitions, sorts, and groups by key2.
  - **Reduce**: User-defined `reduce(key2, list(value2)) → list(key3, value3)`.
  - Optional: Combiner (local reduce to cut shuffle volume), Partitioner (controls key distribution).

- **Data Flow** (Integrated with HDFS + YARN):
  - Input splits align with HDFS blocks → one map task per split.
  - Tasks scheduled with **data locality** preference (node/rack where block resides).
  - Map outputs written locally → shuffled over network to reducers.
  - Reducers write final output to HDFS (via OutputFormat).

- **Fault Tolerance**: Failed tasks re-executed (up to configurable attempts). OutputCommitter handles atomic commits.

**Essential for Toy**: Core phases (split → map → shuffle/sort → reduce → output), basic Input/OutputFormats (e.g., text/line-based), data locality scheduling, task retry. Omit: speculative execution, advanced counters, distributed cache, compression, multiple output formats.

### 3. Key Features to Realize in the "Toy" Hadoop (MVP Prioritization)

The goal is an MVP that **realizes Hadoop's essence** — reliable distributed storage + high-performance parallel processing — while being implementable by a coding agent. Focus on **performance-preserving primitives** (block-based I/O, replication pipelines, data-local task scheduling) and drop everything else.

**Must-Have (Core for Functionality + Performance)**:
1. **HDFS Core**:
   - Block-based file storage (configurable block size, e.g., 128 MB).
   - Replication (default 3; pipeline writes).
   - Basic NameNode (namespace + block map; simple persistence) + DataNodes (storage + heartbeats/block reports).
   - Data locality for reads; basic rack-aware (or node-different) replica placement.
   - Fault tolerance: detect "dead" nodes, re-replicate blocks, checksums.

2. **Simplified Resource Management (YARN-lite or classic-style)**:
   - Central manager for resource arbitration.
   - Per-node workers managing execution slots/containers.
   - Per-job coordinator (AM-like) that requests resources and monitors tasks.

3. **MapReduce Engine**:
   - Input splitting aligned to HDFS blocks.
   - Parallel map tasks (data-local scheduling).
   - Shuffle/sort + reduce phase.
   - Task retry on failure.
   - Output to HDFS.

**Dataflow Extraction Priorities for AI Pipeline**:
- File ingestion → block splitting + replication.
- Job submission → split creation → task scheduling with locality → map → shuffle → reduce → commit.

**Nice-to-Have but Defer (Fewer Features)**:
- Full YARN schedulers, HA, federation, security (Kerberos/Ranger), snapshots, quotas, multiple Input/OutputFormats, streaming/other engines (Spark/etc.), speculative execution, counters, compression.

**Why This Keeps Performance**:
- Data locality + large blocks → high throughput (avoids network bottlenecks).
- Replication + re-execution → fault tolerance without downtime.
- Parallelism via block/task distribution → scales with cluster size.

This toy version would be a functional, performant mini-Hadoop suitable for validation/experimentation. It captures the architectural invariants (master metadata + distributed data/work, automatic recovery, locality) that make Hadoop powerful, making it an ideal target for AI-driven codebase reconstruction. Subsequent iterations could layer on advanced features extracted iteratively from the full codebase.