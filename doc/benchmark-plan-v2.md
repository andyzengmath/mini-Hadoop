# mini-Hadoop vs Apache Hadoop: Benchmark Plan v2 (Distributed-System Extensions)

**Date:** 2026-04-21
**Purpose:** Extend benchmark-plan.md (v1) with distributed-system stress tests requested by reviewer — multi-node failure + restart, large-file I/O (500MB, 1GB), master (NameNode/ResourceManager) restart, mid-write pipeline failover, sustained load.
**Relationship to v1:** v1 tests S1-S4, F1-F4, M1-M4, R1-R5, C1-C3 are kept. v2 adds **L1-L3** (large file), **F5-F10** (fault-tolerance extensions), **D1** (sustained load).

---

## 1. Gaps in v1 the Reviewer Flagged

| Reviewer ask | v1 coverage | Gap |
|---|---|---|
| "3 servers, turn off 1 worker, still works" | F2 reads after death | **No unchanged-SHA check after node restart** |
| "3 servers, turn off 1 master, still can read" | — | **No NameNode / RM restart test** |
| "3 servers, turn off 2, restart 2, file unchanged" | F4 kills 2 but never restarts nor verifies SHA | **No restart + content match** |
| "500MB read/write" | S1/S2 script supports 500MB but sample run only did 10/100MB | **500MB + 1GB not actually run** |

---

## 2. New Benchmark Tests

### 2.1 Large File I/O (L1-L3)

These specifically target the 500MB+ range to expose streaming-vs-buffered I/O behavior, which we know from `benchmark-deep-analysis.md` is where Apache Hadoop's native `libhadoop` should win.

#### L1 — 500MB & 1GB Sequential Write

```
Sizes: 500MB, 1024MB (1GB)
Runs: 3 per size (first discarded, average remaining 2 — fewer runs because each is slow)
Metric: wall-clock seconds, throughput in MB/s, SHA-256 of local input vs read-back
Pass criterion: All SHA-256 checksums match
```

**Commands** (both systems):
```bash
# mini-Hadoop
dd if=/dev/urandom bs=1M count=500 > /tmp/l1-500m.dat
time hdfs put /tmp/l1-500m.dat /bench/l1-500m.dat

# Hadoop
time hdfs dfs -put -f /tmp/l1-500m.dat /bench/l1-500m.dat
```

#### L2 — 500MB & 1GB Sequential Read

Read files written in L1. Same sizes, same methodology. Verify SHA-256 matches.

#### L3 — 500MB Concurrent Write (2 clients)

Tests whether 2 parallel 500MB writes complete correctly — exercises pipeline buffering under memory pressure. This is the most likely place to hit OOM in mini-Hadoop (per memory note: "streaming writes" was still on the remaining-work list).

```
Clients: 2
File size: 500MB each (total: 1GB written simultaneously)
Metric: both SHA-256 match, wall-clock for both to complete
```

---

### 2.2 Fault-Tolerance Extensions (F5-F10)

#### F5 — Master (NameNode) Restart, Read Survives

**Reviewer ask:** "turn off 1 master server, still can read"

Mini-Hadoop has only 1 NameNode (no Active/Standby HA). We test the degraded variant: kill + restart + verify metadata+data survive. This exercises `editlog.go` + `persistence.go`.

```
Procedure:
  1. Write 5 files (10MB each), record their SHA-256.
  2. Stop NameNode container.
  3. Attempt to read any file during NN downtime → EXPECT: fail (no alternate master).
  4. Start NameNode container.
  5. Wait for it to become healthy (poll metrics endpoint or list ops).
  6. Read all 5 files, recompute SHA-256.
  7. Pass: 5/5 match.

Metric: NN restart time (s), number of files recoverable, SHA match.
```

#### F6 — Kill-2/Restart-2 Content Integrity (Colleague's Exact Ask)

```
Procedure:
  1. Write 5 files (10MB each, replication=3). Record SHA-256 locally.
  2. Stop worker-2 and worker-3.
  3. (Optional) Attempt read of all 5 — some may succeed from worker-1 (has 1 replica each).
  4. Restart worker-2 and worker-3.
  5. Wait 20s for DataNode re-registration and heartbeats.
  6. Read all 5 files, compute SHA-256.
  7. Pass: 5/5 SHA-256 match stored originals.

Metric: pass count / 5, time from restart-start to read-success.
```

This is the single most important new test — it exercises disk persistence in DataNodes, block recovery, and NameNode's ability to rediscover blocks on rejoined workers.

#### F7 — Pipeline Failover Mid-Write

Tests whether a write-in-progress survives a worker dying during the write.

```
Procedure:
  1. Start writing a 100MB file.
  2. 2 seconds into the write, stop worker-2 (one of the 3 replicas).
  3. Wait for write to complete (or fail).
  4. Read the file, verify SHA-256.

Expected results:
  - Apache Hadoop: PASS (DFSOutputStream has pipeline recovery).
  - mini-Hadoop:   MAY FAIL (no documented pipeline recovery — we'll document the gap).
```

Implementation note: start the `hdfs put` in the background, `sleep 2`, kill worker-2, `wait` on the write, then verify. Because write speed varies, "2s into write" is approximate — we record whether the kill happened before or after write completion.

#### F8 — ResourceManager Restart Survives Job Submission

Tests that YARN-lite can accept new jobs after RM is restarted.

```
Procedure:
  1. Submit a small WordCount job, wait for completion.
  2. Stop ResourceManager container.
  3. Submit a new WordCount job → EXPECT: fail (no RM).
  4. Start ResourceManager.
  5. Wait for NodeManagers to re-register.
  6. Submit another WordCount job → EXPECT: success.

Metric: RM restart time, time until next job can be submitted, job correctness.
```

#### F9 — Cascade Recovery (kill, wait for re-replication, kill another)

Tests whether re-replication actually produced 3 replicas on the remaining 2 nodes so we can survive a 2nd failure.

```
Procedure:
  1. Write 1 file (10MB, replication=3). Blocks are on [w1,w2,w3].
  2. Kill worker-2.
  3. Wait 30s for NameNode to detect death and re-replicate to [w1,w3]. (mini-Hadoop detection ~11s per v1 F1.)
  4. Kill worker-3.
  5. Attempt read.

Expected:
  - If re-replication completed: read succeeds (1 replica on w1, 1 on w3-before-kill, 1 on w1-again via re-rep).
  - If re-replication too slow: may fail.
```

For mini-Hadoop we may need to configure re-replication, if it exists. If not, this test will expose another gap.

#### F10 — Rolling Restart Under Write Load

Production-style upgrade: restart one worker at a time while writes are ongoing. All writes should succeed (replication=3 tolerates 1 node down).

```
Procedure:
  1. Start a loop: write 1MB file, sleep 1s, repeat for 60s.
  2. In parallel: kill worker-1 at t=15s, restart at t=25s. Kill worker-2 at t=35s, restart at t=45s. Kill worker-3 at t=55s.
  3. After loop ends, list uploaded files, verify SHA-256 for each.

Metric: writes_attempted / writes_succeeded / SHA-mismatches.
```

---

### 2.3 Sustained Load (D1)

#### D1 — 10-minute Mixed Workload

Tests for goroutine leaks, file-descriptor exhaustion, memory growth, GC pauses.

```
Procedure:
  For 10 iterations (≈10 minutes total):
    Round N:
      - Write 10× 10MB files → /bench/d1/round-N/
      - Read 10× prior-round files (if exist)
      - Record timing, memory usage (docker stats snapshot)
  
Metric:
  - Round-over-round throughput (is it stable?)
  - Peak memory (is it growing?)
  - Any failures
```

---

## 3. Test Execution Matrix

| Test | mini-Hadoop | Apache Hadoop | Priority | Est. time |
|---|---|---|---|---|
| L1 500MB write | ✓ | ✓ | HIGH (reviewer ask) | 3m |
| L1 1GB write | ✓ | ✓ | MED | 5m |
| L2 500MB/1GB read | ✓ | ✓ | HIGH | 5m |
| L3 concurrent 500MB | ✓ | ✓ | MED | 3m |
| F5 NameNode restart | ✓ | ✓ | HIGH (reviewer ask) | 2m |
| F6 kill-2/restart-2 | ✓ | ✓ | **CRITICAL** (reviewer ask) | 2m |
| F7 pipeline failover | ✓ | ✓ | MED (likely reveals gap) | 2m |
| F8 RM restart | ✓ | ✓ | MED | 3m |
| F9 cascade recovery | ✓ | ✓ | LOW | 2m |
| F10 rolling restart | ✓ | ✓ | MED | 3m |
| D1 sustained load | ✓ | ✓ | LOW | 10m |
| **Total per system** | | | | **~40m** |
| **Both systems** | | | | **~80m** |

---

## 4. Expected Results Hypothesis

| Test | mini-Hadoop (predicted) | Apache Hadoop (predicted) | Notes |
|---|---|---|---|
| L1 500MB write | ~20-30s (~20 MB/s) | ~10-20s (~30-50 MB/s) | Hadoop native I/O wins at scale |
| L1 1GB write | ~40-60s | ~25-40s | Same |
| L2 500MB read | ~10-15s (~35-50 MB/s) | ~5-10s (~50-80 MB/s) | Same |
| L3 2×500MB concurrent | likely PASS, may expose buffering bug | PASS | Mini has had streaming-write issues |
| F5 NN restart | restart ~3-5s, read PASS | restart ~20-30s, read PASS | Go cold-start vs JVM |
| F6 kill-2/restart-2 | **PASS 5/5** | **PASS 5/5** | Core replication works on both |
| F7 pipeline failover | **may FAIL** | PASS | Mini has no pipeline recovery (gap) |
| F8 RM restart | PASS | PASS | Re-submission should work for both |
| F9 cascade recovery | PASS **if** re-replication works | PASS | Re-replication is the Achilles heel |
| F10 rolling restart | ~95%+ writes succeed | ~95%+ writes succeed | Both tolerate 1 down |
| D1 sustained | stable throughput | stable throughput | Both should be leak-free |

---

## 5. Reproduction

```bash
# All new benchmarks (both systems, ~80 min)
./scripts/benchmark-v2.sh all

# mini-Hadoop only
./scripts/benchmark-v2.sh mini-only

# Quick validation (smaller files, shorter D1)
./scripts/benchmark-v2.sh quick

# Single test category
./scripts/benchmark-v2.sh all L1     # only L1 on both
./scripts/benchmark-v2.sh mini-only F6  # only F6 on mini
```

Output: `benchmark_v2_results_YYYYMMDD_HHMMSS/` with per-test `.txt` files + `all_results.txt` summary.

---

## 6. Results Template

### Large File (L1-L3)

| Test | mini-Hadoop | Apache Hadoop | Winner | Notes |
|---|---|---|---|---|
| L1 500MB write (s, MB/s) |  |  |  |  |
| L1 1GB write (s, MB/s) |  |  |  |  |
| L2 500MB read (s, MB/s) |  |  |  |  |
| L2 1GB read (s, MB/s) |  |  |  |  |
| L3 2×500MB concurrent |  |  |  |  |

### Fault Tolerance (F5-F10)

| Test | mini-Hadoop | Apache Hadoop | Winner | Notes |
|---|---|---|---|---|
| F5 NN restart time (s) |  |  |  |  |
| F5 files SHA match after restart |  |  |  |  |
| F6 kill-2/restart-2 (SHA match) |  |  |  |  |
| F7 pipeline failover (PASS/FAIL) |  |  |  |  |
| F8 RM restart + re-submit |  |  |  |  |
| F9 cascade recovery (PASS/FAIL) |  |  |  |  |
| F10 rolling restart (writes succeeded / attempted) |  |  |  |  |

### Sustained Load (D1)

| Metric | mini-Hadoop | Apache Hadoop |
|---|---|---|
| Round 1 write throughput (MB/s) |  |  |
| Round 10 write throughput (MB/s) |  |  |
| Peak memory, idle (MB) |  |  |
| Peak memory, round 10 (MB) |  |  |
| Memory growth (MB) |  |  |
| Failures total |  |  |

---

## 7. Known Architectural Gaps (Expected to Show Up)

Documenting what we expect v2 to reveal, so results are interpretable:

- **No NameNode HA** — F5 is a restart test, not an Active/Standby failover. Apache Hadoop with HA would show zero downtime, but we're comparing single-master to single-master for apples-to-apples.
- **Pipeline recovery in mini-Hadoop** — based on the open PR work, `pkg/hdfs/ackqueue.go` exists but mid-pipeline-failure recovery is not yet validated. F7 will confirm/deny.
- **Re-replication scheduler** — mini-Hadoop's deep-analysis doc mentions "re-replication start: ~11s" for F1. F9 tests whether re-replication **completes** fast enough to survive a second failure.
- **Streaming writes** — memory note says "streaming writes" was an open item. L1 at 500MB may reveal if a 128MB in-memory buffer per block exists.

---

## 8. Relationship to Hadoop's Native Test Suite

For reference, Hadoop's own distributed-system tests in its repo (`hadoop-hdfs-project/.../test/`):

| Hadoop test | Our equivalent |
|---|---|
| `TestDatanodeDeath.java` | F6 (kill-2/restart-2) + F2 (v1) |
| `TestDatanodeRestart.java` | F5 + F6 |
| `TestDFSIO.java` | L1 + L2 (write/read throughput) |
| `TestFailoverController.java` | F8 (RM restart — our version is simpler, no ZK) |
| `TestPipelinesFailover.java` | F7 |

We don't run Hadoop's JUnit tests directly (they require MiniDFSCluster Java setup); our shell-based equivalents run against a real 3-node Docker cluster, which is closer to a real-world deployment test.
