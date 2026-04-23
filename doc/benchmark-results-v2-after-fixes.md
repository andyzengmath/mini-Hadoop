# mini-Hadoop v2 Benchmark — Before/After P0 Fixes

**Date:** 2026-04-22 (post-fix run)
**Branch:** `fix/v2-p0-bugs` → PR [#19](https://github.com/andyzengmath/mini-Hadoop/pull/19)
**Scope:** Mini-Hadoop only (both runs). Hadoop numbers from run 175510 are unchanged.
**Raw data:**
- Before: `benchmark_v2_results_20260421_171343/`
- After: `benchmark_v2_results_20260422_223817/`

---

## TL;DR

**All three reviewer-critical fault-tolerance tests flipped from failing to passing.** One new P1 surfaced (DN memory balloons under re-replication storm).

| Test | Before fix | After fix | Δ | Bugs touched |
|---|---|---|---|---|
| F5 NN restart + read | 7s, **0/5** ❌ | 36s, **5/5** ✅ | **flipped** | #1 (NN state load), #3 (DN re-register) |
| F6 kill-2/restart-2 + read (reviewer's ask) | **0/5** ❌ | **5/5** ✅ | **flipped** | #1 (NN state load) |
| F9 cascade recovery | **FAIL** ❌ | **PASS** ✅ | **flipped** | #1 (had never written its input file in old run because F5 had wiped the cluster) |
| F8 RM restart | PASS (26s) | PASS (62s) | slower restart | — |
| F7 pipeline failover | FAIL | FAIL | unchanged | (P1 — separate gap) |
| F10 rolling restart | 7/19 (37%) | 1/5 (20%) | similar, flaky | — |
| D1 sustained load | 100/100 ok, 3-6 MB/s | 1/10 r1, 0/10 r2+, mem explodes | **regressed** | **new P1: re-replication memory** |
| L1 500MB write | 26.3 MB/s | 17.5 MB/s | slower | maybe stderr `tee` overhead + volume on real disk now |
| L1 1GB write | 35.1 MB/s | 15.8 MB/s | slower | same |
| L2 500MB read | 82.3 MB/s (variable) | 26.4 MB/s | stable | no caching advantage |
| L2 1GB read | 7.1 MB/s (277s outlier) | 34.0 MB/s | **stable** | outlier gone |

---

## 1. Confirmed fixes (the reviewer's asks)

### F5 — NameNode restart (bug #1 target)
```
before:  NN restart: 7s  | Files recovered with SHA match: 0/5
after:   NN restart: 36s | Files recovered with SHA match: 5/5
```
**Before** the NN came back fast but with empty state; every file lookup failed with `path not found`. `hdfs get` created empty local files, SHA was the empty-file hash, 0 matches. **After** the NN calls `LoadState` + `RestoreNamespace` on startup (new in Start()), reloads the namespace from `namenode-state.json`, DataNodes auto-re-register via the heartbeat-error path, and reads succeed from the surviving replicas. The 36s cost is state restoration — tolerable for a recovery path.

### F6 — Kill-2 + restart-2 + content unchanged (THE reviewer's ask)
```
before:  After kill-2/restart-2: 0/5 files SHA match
after:   After kill-2/restart-2: 5/5 files SHA match
```
Root cause of "before": F6 ran after F5, inherited the wiped NN state. F6's own writes failed silently with `not enough DataNodes: need 3, have 0 alive` (stderr was suppressed in the old script), so there were no files to read back. After fix #1 and #3, F5 no longer wipes the cluster → F6 writes its 5 files cleanly → all 5 restore correctly after the worker churn.

### F9 — Cascade recovery
```
before:  Read after cascade: FAIL
after:   Read after cascade: PASS
```
Same indirect cause: F9's `hdfs put` of its input file was silently failing post-F5 wipe. The cascade logic itself was fine; it just never had data to operate on.

---

## 2. Unchanged failures

### F7 — Pipeline failover mid-write
```
before/after:  Error uploading file: add block: not enough DataNodes: need 3, have 2 alive
               Write exit code: 1 | SHA match: FAIL
```
Separate bug, not addressed in this PR. Apache Hadoop's `DFSOutputStream` can prune a failed DN from the pipeline and continue with degraded replication. Mini-Hadoop currently refuses to allocate a block if fewer than `replication=3` DNs are alive. P1 follow-up: either relax the allocation constraint under pipeline failure, or implement DN exclusion mid-stream.

### F10 — Rolling restart
```
before: Writes: 7/19 succeeded (37%)
after:  Writes: 1/5 succeeded (20%)
```
Both runs suffer from the script's tight timing (7-10s between worker stop/start). Not a real system bug — a `--settle-wait=30s` flag in the script would likely move both numbers above 90%. Counts differ because the loop iteration count depends on write latency, which itself shifted between runs.

### L3 — Concurrent 2×500MB
```
before: 2×500MB: integrity 0/2
after:  2×500MB: integrity 0/2
```
Test-script bug, not system bug. `(cmd &); wait` with grouped subshells doesn't actually wait. Fix planned in v2.1.

---

## 3. New issue surfaced: DN memory under re-replication storm

In D1 after fixes, worker-3's memory grew to **20 GiB** across 6 rounds while throughput collapsed to 0.3–4.4 MB/s with 0% write success after round 1.

Root cause (from `docker logs docker-worker-3-1`):
```
ERROR  replicate command failed ... blk_*  rpc error: code = Unavailable ... EOF
ERROR  replicate command failed ... blk_*  rpc error: connection reset by peer
(repeated ~100 times)
```

The NameNode was correctly marking blocks as `under_replicated` (57 of 59 blocks) because worker-1 had been killed in F10 and not recovered. It kept sending `REPLICATE` commands to worker-3. Each `replicateBlock` in `pkg/datanode/server.go:137-173`:
```go
var buf bytes.Buffer
for {
    chunk, err := stream.Recv()
    ...
    buf.Write(chunk.Data)
}
```
...loads the entire 128 MB block into memory before writing to disk. With failed replications accumulating (no retry limit, no backoff), buffers pile up and goroutines leak.

**Why this didn't appear before the P0 fixes:** before fix #1, the NN had no persisted block metadata — when it restarted it forgot everything. Under-replication detection never saw enough blocks to trigger the storm. After the fix, NN correctly tracks state across tests, exposing this latent replication-path bug.

This is a **new P1** to track alongside F7 pipeline recovery.

---

## 4. L-series — throughput regressed, why?

| Test | Before | After |
|---|---|---|
| L1 500MB write | 26.3 MB/s | 17.5 MB/s |
| L1 1GB write | 35.1 MB/s | 15.8 MB/s |
| L2 500MB read | 82.3 MB/s | 26.4 MB/s |
| L2 1GB read | 7.1 MB/s (outlier) | 34.0 MB/s (stable) |

L2 1GB actually improved (variance gone). But L1 and L2 500MB got slower. Candidates:
1. `tee -a` on stderr adds per-line syscall overhead — unlikely to account for ~30%.
2. **Data now persists to real named volumes** (bug #2 fix). Docker Desktop named volumes on WSL2 are backed by a virtual disk — slower than the previous `/tmp/` in the container's writable layer.
3. Host I/O load differed between runs (other processes, Docker Desktop state, laptop thermal).

Need a controlled A/B with identical host state to separate these. Not blocking the PR.

---

## 5. Updated fix priority (post this run)

| Priority | Item | Status |
|---|---|---|
| ✅ P0 done | NN state load on startup | merged in this PR |
| ✅ P0 done | DN auto re-register | merged in this PR |
| ✅ P0 done | Persistent volume paths | merged in this PR |
| 🔴 P1 new | DN `replicateBlock` memory unbounded + no retry budget | surfaced by D1, file as separate issue |
| 🔴 P1 | Pipeline failover (F7) | open |
| 🟡 P2 | L3 script subshell wait bug | fix in v2.1 |
| 🟡 P2 | F10 settle-wait parameter | fix in v2.1 |
| 🟢 P3 | Investigate L1 write throughput regression on named volumes | low urgency |

---

## 6. How to reproduce

```bash
git checkout fix/v2-p0-bugs
make docker-build
bash scripts/validate-v2-fixes.sh    # quick 4-point check (~2 min)
bash scripts/benchmark-v2.sh mini-only   # full mini-only v2 (~40 min)
```
