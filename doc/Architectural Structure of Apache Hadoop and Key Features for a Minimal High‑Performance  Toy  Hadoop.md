# Architectural Structure of Apache Hadoop and Key Features for a Minimal High‑Performance "Toy" Hadoop

## Executive Summary

Apache Hadoop is a distributed data processing framework whose core architecture consists of three tightly coupled layers: the Hadoop Distributed File System (HDFS) for storage, YARN for resource management, and MapReduce (or other compute engines) for batch processing. These components work together in a master–slave cluster design to provide scalable, fault‑tolerant processing of very large datasets on commodity hardware.[^1][^2][^3][^4][^5]

For an AI‑driven codebase rebuild that aims to implement a "toy" Hadoop preserving key performance characteristics while dropping non‑essential features, the minimum viable subset centers on: HDFS‑like block storage with replication and data locality; a simple yet locality‑aware resource and job orchestration layer (YARN‑like or classic JobTracker/TaskTracker); and a MapReduce‑style execution pipeline including input splitting, map, shuffle/sort, and reduce. Non‑core ecosystem services (Hive, HBase, security extensions, sophisticated schedulers, multi‑tenant features) can be excluded from the first reconstruction without compromising the core performance model.[^6][^7][^3][^8][^9][^1]


## Hadoop Architectural Overview

### Design Goals

Hadoop was designed to provide an inexpensive, reliable, and scalable framework for storing and analyzing massive data on clusters of commodity machines. It targets workloads where applications write very large files once and read them many times at streaming speeds, tolerating individual node failures transparently. The architecture explicitly co‑locates storage and compute to maximize aggregate bandwidth via data locality, scheduling computation on nodes where data blocks reside whenever possible.[^7][^3][^4][^10][^1]

### High‑Level Layering

Most descriptions of Hadoop architecture converge on a three‑layer structure.[^2][^5][^1]

- Storage layer: HDFS, a distributed file system exposing a POSIX‑like file abstraction backed by large replicated blocks on DataNodes.
- Resource management layer: YARN in Hadoop 2.x+, or the classic JobTracker/TaskTracker model in Hadoop 1.x.[^8][^9]
- Processing layer: MapReduce as the canonical batch compute engine, with other engines (e.g., Spark) later running on the same YARN substrate.[^4][^2]

A fourth category, Hadoop Common, aggregates shared utilities and libraries used by all components.[^5]


## HDFS: Storage Layer

### Core Responsibilities and Semantics

HDFS stores very large files as sequences of fixed‑size blocks distributed across cluster nodes, with each block replicated for fault tolerance. Files are typically write‑once and read‑many, and HDFS enforces a single writer per file to simplify consistency and enable high‑throughput streaming writes and reads. The system is optimized for large sequential I/O rather than low‑latency random reads of small files.[^11][^3][^10][^12]

Key semantics include:
- Large, configurable block size (e.g., 64 MB or 128 MB by default) to reduce metadata overhead and increase throughput.[^13][^3][^1]
- Per‑file replication factor, configurable at creation time and adjustable later, balancing reliability, availability, and storage cost.[^3][^10][^11]
- Rack‑aware placement policies for replicas to trade off write cost against reliability and aggregate read bandwidth.[^12][^3]

### Master–Slave Structure: NameNode and DataNodes

HDFS uses a master–slave design in which a single NameNode manages filesystem metadata (namespace, block map) and a set of DataNodes store actual block contents on their local file systems.[^1][^3][^12]

- NameNode: Holds the directory tree, file‑to‑block mapping, and replication state in memory and logs changes to durable storage.[^3][^12]
- DataNodes: Store block replicas as files and periodically send block reports and heartbeats to the NameNode.[^10][^3]

This division allows the NameNode to make global replication and placement decisions while DataNodes focus on local storage and streaming I/O.[^10][^3]

### Data Placement, Replication, and Fault Tolerance

On file creation, HDFS splits data into blocks, then places replicas according to a policy that typically puts one replica on the local (writer) node, another on a different node in a remote rack, and additional replicas on other racks to balance locality and resilience. Replication is orchestrated by the NameNode based on heartbeats and block reports from DataNodes, which allow detection of failed nodes and under‑replicated blocks.[^12][^3][^10]

Fault tolerance is achieved by:
- Re‑replicating blocks from healthy replicas when replicas fall below the configured factor.
- Preferring nearby replicas on reads (same node, same rack, then same data center) to minimize latency and cross‑rack bandwidth.[^3]

### HDFS Dataflows: Write and Read Paths

The HDFS write path is optimized for streaming writes with pipelined replication.

- When a client writes a file, it contacts the NameNode to obtain a list of DataNodes for the first block, then streams data to the first DataNode, which forwards it to the next DataNode in a replication pipeline until the configured number of replicas is written.[^10][^3]
- Local staging caches the first chunk of data to support the memory hierarchy and avoid blocking on remote endpoints, then the pipeline is established once enough data accumulates.[^10]

The read path is symmetrical but simpler:

- The client asks the NameNode for block locations, then reads from the nearest replica via a streaming interface, relying on the placement policy to provide good locality.[^3]

These dataflows are central to Hadoop’s high throughput, as they minimize coordination overhead and leverage large block sizes and co‑located storage.


## YARN and Classic MapReduce Resource Management

### Hadoop 1.x: JobTracker and TaskTrackers

In Hadoop 1.x, MapReduce itself encompassed the resource management layer via a JobTracker (master) and multiple TaskTrackers (workers).[^14][^9][^8]

- JobTracker: Coordinates all jobs, maintains job metadata, splits jobs into tasks, and assigns tasks to TaskTrackers based on data locality and load balancing.[^14][^8]
- TaskTrackers: Run map and reduce tasks in child JVMs, report progress and heartbeats back to the JobTracker, and handle task‑level failure with retries.[^15][^8][^14]

This architecture provides global scheduling and monitoring but centralizes all coordination in a single JobTracker process, which can become a scalability bottleneck for very large clusters.[^9]

### Hadoop 2.x+: YARN (Yet Another Resource Negotiator)

YARN generalizes resource management beyond MapReduce by splitting JobTracker responsibilities into a ResourceManager and per‑application ApplicationMasters.[^2][^4][^9]

- ResourceManager: Manages cluster‑wide resources (CPU, memory) and handles container allocation on NodeManagers according to policies.[^4][^2]
- NodeManagers: Per‑node agents responsible for launching and monitoring containers, reporting resource usage and liveness to the ResourceManager.[^2]
- ApplicationMaster: Per‑job (or per application) component responsible for negotiating resources with the ResourceManager and orchestrating tasks for that application (e.g., a MapReduce job).[^9][^2]

This decomposition improves scalability, supports multiple processing frameworks on the same cluster, and isolates failures of individual ApplicationMasters from the global ResourceManager.[^9][^2]

### Resource Management Dataflows

The resource management layer coordinates several recurring dataflows that are critical for performance and resilience:

- Heartbeats from TaskTrackers or NodeManagers to the JobTracker/ResourceManager supply liveness and resource usage information used to schedule new tasks or containers.[^8][^14][^9]
- Job submission and initialization: Clients submit job metadata and binaries to the master (JobTracker or ResourceManager/ApplicationMaster), which then determines input splits and assigns work to workers, preferably on nodes with local HDFS blocks.[^7][^14][^9]
- Failure handling: When heartbeats or progress reports fail, the master marks tasks or nodes as failed and reschedules tasks on other nodes with available replicas.[^14][^8]


## MapReduce: Processing Layer

### Programming Model

MapReduce exposes a simple programming model with two main user‑defined functions:

- Map: Consumes input key–value pairs, performs user logic (e.g., parsing, filtering, mapping), and emits intermediate key–value pairs.
- Reduce: Receives grouped intermediate values for each key and aggregates them into final results, such as sums, counts, or more complex operations.[^6][^5][^7][^4]

This model is well suited for batch analytics on large, partitionable datasets and enables automatic parallelization and distribution across cluster nodes.[^5][^4]

### Execution Pipeline and Dataflows

The MapReduce execution pipeline is composed of several stages, each with distinct dataflows and performance characteristics.[^15][^6][^7][^14][^9]

1. Job submission: The client packages the job’s jar, configuration, and input/output paths and submits them to the JobTracker or ApplicationMaster.[^6][^14]
2. Input splitting: The master calculates input splits based on HDFS block boundaries and assigns splits to mappers, aligning splits with block locations to maximize locality.[^7][^6]
3. Map tasks: TaskTrackers or containers execute map tasks that read input splits from local HDFS, emit intermediate key–value pairs, and buffer them in memory.[^6][^7]
4. Shuffle and sort: Map outputs are partitioned by reducer key, spilled to disk as needed, then fetched by reducers over the network; during fetch, intermediate results are grouped by key and sorted.[^7][^9][^6]
5. Reduce tasks: Reducers run user‑defined reduce functions over grouped values and write final output to HDFS, typically as one file per reducer.[^6][^7]

The shuffle and sort phase is often the most expensive part of MapReduce and is essential for aggregating data correctly and balancing reducer workloads.[^9][^7][^6]

### Fault Tolerance in MapReduce

MapReduce tolerates task and node failures via re‑execution.

- Task failures are detected through missed heartbeats or error status; failed map or reduce tasks are rescheduled on other nodes, potentially re‑reading input from HDFS or re‑fetching map outputs.[^8][^14][^9]
- Job‑level progress and counters are tracked at the master, which reissues tasks until completion or until the job is deemed failed after exceeding retry limits.[^14][^8]

Because MapReduce inputs reside in HDFS and intermediate state can be recomputed, the framework avoids complex distributed checkpointing at the task level.[^4][^7]


## Hadoop Common and Ecosystem Components

### Hadoop Common Utilities

Hadoop Common comprises shared libraries and utilities used across HDFS, YARN, and MapReduce, including configuration APIs, RPC frameworks, serialization mechanisms, and native code optimizations. These utilities provide consistent configuration and communication semantics across components, enabling cross‑module tooling and administration.[^5]

### Higher‑Level Ecosystem

Above the core Hadoop layers lies a rich ecosystem of higher‑level tools such as Hive (SQL‑like querying), HBase (NoSQL store), Pig (dataflow language), and others, which run on top of HDFS and typically use MapReduce or other engines under the hood. While critical to many production deployments, these ecosystem tools are conceptually distinct from the core storage, resource management, and batch processing stack.[^4][^5]


## Identifying Key Features for a Minimal High‑Performance "Toy" Hadoop

### Principles for Feature Selection

For an AI‑native rebuild where the aim is to preserve Hadoop’s essential performance characteristics—linear scalability, high throughput via data locality, and robust fault tolerance—while dropping peripheral complexity, feature selection should be guided by several principles:

- Preserve architecture‑critical invariants (e.g., write‑once/read‑many, large blocks, co‑located storage and compute).
- Maintain the essential control and dataflows (HDFS I/O, job submission, input splitting, shuffle/sort, task scheduling, failure recovery).
- Simplify or omit cross‑cutting concerns that are not fundamental to performance (complex security models, pluggable schedulers, multi‑tenant isolation, advanced rack‑aware policies beyond a basic heuristic).

### Essential HDFS‑Like Features

A minimal yet performance‑faithful storage layer should implement:

- Distributed block storage: Files split into large, fixed‑size blocks (e.g., ≥64 MB) stored across worker nodes.[^13][^1][^3]
- Replication: Per‑file replication factor with automatic re‑replication on node or block failure, driven by periodic heartbeats and block reports.[^11][^3][^10]
- Simple placement policy: At least a basic policy that places one replica on the writer node and others on different nodes, approximating rack diversity without full topology modeling.[^12][^3]
- Streaming read/write APIs: Sequential, high‑throughput streaming interfaces for reading and writing files, optimized for large transfers rather than small random I/O.[^3][^10]
- Centralized metadata master: A NameNode‑like component that maintains namespace, block mapping, and replication state in memory with durable logs.[^12][^3]

Non‑essential features that can be de‑scoped in a first iteration include:

- Full POSIX semantics, appends, and complex permissions models.
- Sophisticated rack and data center awareness beyond a simple multi‑node placement heuristic.
- Federation and high‑availability NameNode arrangements.

### Essential Resource Management Features

A minimal resource management layer can adopt either a simplified YARN‑like architecture or a classic JobTracker/TaskTracker design, but must keep core scheduling and monitoring semantics:

- Central scheduler: A master component that tracks worker liveness and resource capacity, scheduling tasks (or containers) preferably on nodes that host required HDFS blocks.[^8][^14][^7]
- Worker agents: Lightweight daemons on each node capable of launching and supervising task processes, reporting heartbeats and resource usage.[^14][^8][^9]
- Locality‑aware scheduling: Basic policies that prefer node‑local or rack‑local execution to maximize data locality and throughput.[^7][^4]
- Simple queuing and fairness: A minimal queue structure for job submission and scheduling; sophisticated multi‑queue, preemption, or multi‑tenant isolation mechanisms can be omitted initially.

Given the experiment’s focus, classic JobTracker/TaskTracker may be easier to reconstruct faithfully than full YARN, while still capturing Hadoop’s original performance characteristics.[^8][^9][^14]

### Essential MapReduce Processing Features

To preserve the MapReduce performance profile, the "toy" implementation should support:

- Input splitting based on HDFS block boundaries, exposing splits to mappers to align compute with storage.[^6][^7]
- Map and reduce task lifecycle: Task instantiation, progress reporting, retries on failure, and clean termination.[^14][^8][^6]
- Shuffle and sort: Network transfer of map outputs to reducers, grouping by key, and sorting keys before reduce, as this stage dominates performance and correctness for many workloads.[^9][^7][^6]
- Simple partitioner and combiner interfaces: Pluggable partitioning by key to distribute work across reducers and optional combiners for pre‑aggregation on mappers.[^7]

User‑visible APIs can be reduced to a subset (e.g., key‑value interface and simple configuration) while still capturing the essential execution pipeline.

### Core Dataflows to Preserve

From a codebase analysis and reconstruction standpoint, the following dataflows represent the backbone of Hadoop’s behavior and should be modeled explicitly:

1. HDFS file write dataflow: Client → NameNode (create + block allocation) → DataNode pipeline for streaming data and replicas → NameNode block reports and replication state updates.[^10][^3]
2. HDFS file read dataflow: Client → NameNode (block locations) → nearest DataNode → streaming read.[^3]
3. Heartbeat and monitoring dataflow: DataNodes and TaskTrackers/NodeManagers → NameNode and JobTracker/ResourceManager, carrying liveness and block/task state.[^8][^9][^3]
4. Job submission and initialization: Client → JobTracker/ResourceManager & ApplicationMaster (job metadata, jar, configuration) → input split computation and task assignment.[^6][^14][^7]
5. Task execution and progress: Task processes → TaskTrackers/ApplicationMaster (status, counters, intermediate output location).[^9][^14][^8]
6. Shuffle/sort dataflow: Mappers spill outputs → reducers fetch via HTTP/RPC → merge, group, sort, then invoke reduce.[^7][^9][^6]

These flows collectively define the system’s control plane and data plane; capturing them is more important for faithful performance than reproducing every configuration option or plugin.


## Suggested De‑Scoping for a First "Toy" Implementation

To make the AI‑driven rebuild tractable while preserving performance‑critical behavior, several areas can be intentionally simplified:

- Security and multi‑tenancy: Authentication, authorization, and multi‑tenant isolation (Kerberos, ACLs, queue capacity policies) can be stubbed or replaced with a simple trust model, as they primarily affect access control rather than raw throughput.[^5][^4]
- Complex scheduling policies: Capacity, fair scheduling, preemption, and pluggable schedulers can be reduced to a basic FIFO or priority queue with locality awareness.[^2][^9]
- Ecosystem integrations: Higher‑level tools such as Hive, HBase, Pig, and external connectors can be ignored in the initial reconstruction, focusing solely on core HDFS + MapReduce.[^4][^5]
- Administrative tooling: Web UIs, advanced metrics, rolling upgrades, and fine‑grained configuration plumbing can be replaced with minimal logging and metrics sufficient for debugging the toy implementation.


## Implications for an AI‑Native Codebase Rebuild Pipeline

From the perspective of an AI‑native rebuilt implementation, Hadoop’s architecture offers a relatively clean separation between behaviorally critical modules (HDFS data path, scheduler, MapReduce pipeline) and peripheral features.

- Representation: The pipeline should extract and represent HDFS, MapReduce, and resource‑management dataflows as explicit graphs of components and messages (RPCs, heartbeats, block reports, shuffle transfers) since these constitute the core execution semantics.[^10][^3][^9]
- Spec generation: For each key flow, the system can synthesize behavioral specifications (pre/postconditions, invariants like replication factor ≥ N, single writer per file, at‑least‑once task execution) to guide the coding agent.
- Modular reconstruction: The AI agent can re‑implement a reduced but architecturally faithful subset: a NameNode/DataNode pair with block replication, a JobTracker/TaskTracker‑like scheduler, and a MapReduce runtime including shuffle/sort and basic locality‑aware task placement.

Focusing reconstruction on these elements maximizes the chance that the resulting "toy" Hadoop exhibits similar scaling and failure behavior on realistic workloads, even if it omits the rich ecosystem and administrative surface of a full Hadoop distribution.[^1][^2][^3][^9][^7]

---

## References

1. [Apache Hadoop Architecture - HDFS, YARN & MapReduce - TechVidvan](https://techvidvan.com/tutorials/hadoop-architecture/) - Explore Hadoop architecture and the components of Hadoop architecture that are HDFS, MapReduce, and ...

2. [Hadoop Architecture Explained: HDFS, YARN & MapReduce](https://www.linkedin.com/pulse/hadoop-architecture-explained-suraj-kumar-soni-k6pbc) - Understand Hadoop architecture, including HDFS, YARN, and MapReduce, and learn how Hadoop enables sc...

3. [HDFS Architecture Guide - Apache Hadoop](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html) - The blocks of a file are replicated for fault tolerance. The block size and replication factor are c...

4. [What is Apache Hadoop and MapReduce - Azure HDInsight](https://learn.microsoft.com/en-us/azure/hdinsight/hadoop/apache-hadoop-introduction) - Apache Hadoop MapReduce is a software framework for writing jobs that process vast amounts of data. ...

5. [Hadoop - Architecture - GeeksforGeeks](https://www.geeksforgeeks.org/big-data/hadoop-architecture/) - Components of Hadoop Architecture · 1. MapReduce · 2. HDFS · 3. YARN (Yet Another Resource Negotiato...

6. [MapReduce Architecture - GeeksforGeeks](https://www.geeksforgeeks.org/software-engineering/mapreduce-architecture/) - Shuffling groups identical keys, e.g., (Hadoop, 1), (Hadoop, 1), (Hadoop, 1) becomes (Hadoop, [1,1,1...

7. [MapReduce Tutorial - Apache Hadoop](https://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html) - This document comprehensively describes all user-facing facets of the Hadoop MapReduce framework and...

8. [[PDF] Hadoop MapReduce: Review](https://storm.cis.fordham.edu/zhang/cs5950/slides/Summary.pdf) - There are two types of nodes that control the job execution process: a jobtracker and a number of ta...

9. [[PDF] Lecture (4 & 5): MapReduce](https://uomus.edu.iq/uploads/lectures/45/5f3321591e3e4546a5f93d78cd82f245.pdf) - Figure 2: MapReduce Execution Pipeline. 4. JobTracker and TaskTracker in Classic MapReduce ... Step ...

10. [[PDF] HDFS Architecture - andrew.cmu.ed](https://www.andrew.cmu.edu/course/14-848-f19/applications/ln/14848-l10.pdf) - Local caching is intended to support use of memory hierarchy and throughput needed for streaming. Do...

11. [Apache Hadoop 3.3.5 – HDFS Architecture](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html) - The block size and replication factor are configurable per file. All ... It stores each block of HDF...

12. [[PDF] The Hadoop Distributed File System - cs.wisc.edu](https://pages.cs.wisc.edu/~akella/CS838/F15/838-CloudPapers/hdfs.pdf) - Each block replica on a DataNode is represented by two files in the local host's native file system....

13. [HDFS Architecture and Block Size Insights | PDF | Apache Hadoop](https://www.scribd.com/document/869657883/Hadoop-Distributed-File-System-HDFS-Data-Management) - HDFS uses fixed-size blocks to enhance performance and storage efficiency, with larger blocks reduci...

14. [6. How MapReduce Works - Hadoop: The Definitive Guide [Book]](https://www.oreilly.com/library/view/hadoop-the-definitive/9780596521974/ch06.html) - The jobtracker is a Java application whose main class is JobTracker . The tasktrackers, which run th...

15. [Anatomy of MapReduce Job Execution | PDF - Scribd](https://www.scribd.com/document/685569461/ANATOMY-OF-MAP-REDUCE-JOBS-PDF) - It discusses: 1) How a job is submitted and initialized, with the jobtracker coordinating and determ...

