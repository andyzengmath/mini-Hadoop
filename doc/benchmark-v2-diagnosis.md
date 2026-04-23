# v2 Fault-Tolerance Failures — Diagnosis

**Date:** 2026-04-22
**Question:** In v1 benchmarks, mini-Hadoop's fault tolerance worked (F2, F4 passed). In v2, nearly all fault tests failed (F5, F6, F7, F9). Is it a test bug or a real code bug?
**Answer:** **Real code bugs.** Three of them, which compound under v2's harder scenarios. Test script has a minor visibility issue that delayed diagnosis but did not cause the failures.

---

## Reproduction Summary

Running a clean mini-Hadoop cluster and manually stepping through the v2 F5/F6 sequence with stderr fully visible:

| Step | Result |
|---|---|
| 1. Cluster up, write 10MB file with SHA `7dda53fb…` | ✅ success |
| 2. Read back (control) | ✅ SHA matches |
| 3. Kill worker-2 and worker-3 | — |
| 4. Read (2 DNs down, only worker-1 alive) | ✅ **SHA matches** — replication works |
| 5. Restart worker-2 and worker-3 | — |
| 6. Read (after DN restart) | ✅ **SHA matches** — DN restart is fine |
| 7. Stop + start NameNode | — |
| 8. `hdfs ls /` | ❌ **empty — all metadata gone** |
| 9. `hdfs get /t.dat` | ❌ `Error downloading file: get block locations: path not found: "t.dat"` |
| 10. After NN restart, try `hdfs put /f6/f-N.dat` | ❌ `Error uploading file: add block: not enough DataNodes: need 3, have 0 alive` |

Step 8–10 = the v2 F5/F6 failure modes in isolation.

---

## Bug #1 — NameNode doesn't reload state on startup

**Where:** `pkg/namenode/server.go:27-34`

```go
func NewServer(cfg config.Config) *Server {
    return &Server{
        ns:     NewNamespace(),          // ← always empty, never loaded
        bm:     NewBlockManager(cfg.ReplicationFactor, cfg.DeadNodeTimeout),
        cfg:    cfg,
        stopCh: make(chan struct{}),
    }
}
```

**Why it's broken:** `LoadState` and `RestoreNamespace` exist in `pkg/namenode/persistence.go` and are unit-tested in `persistence_test.go`, but **they are never called in the real startup path**. `Start()` (line 37-60) creates an `EditLog` via `NewEditLog` but does not replay its contents. `Stop()` saves state on shutdown but no counterpart loads it on startup.

**Effect:** Every NameNode restart begins with an empty namespace — even though the prior run saved `namenode-state.json` and appended to `edits.log` in the metadata dir.

**Fix sketch:**
```go
// In NewServer or Start:
state, err := LoadState(cfg.MetadataDir)
if err != nil {
    return err
}
if state != nil {
    s.ns = RestoreNamespace(state.Namespace)
    s.bm.RestoreBlocks(state.Blocks)    // needs a new helper
    // Then replay edits.log entries with sequence > state.Timestamp
}
```

---

## Bug #2 — Default data directories don't match Docker volume mounts

**Where:** `pkg/config/config.go:80,83`

```go
MetadataDir: "/tmp/minihadoop/namenode",
DataDir:     "/tmp/minihadoop/datanode",
```

**vs.** `docker/docker-compose.yml`

```yaml
namenode:
  volumes:
    - nn-data:/data/namenode            # ← mount at /data, not /tmp/minihadoop
worker-1:
  volumes:
    - w1-data:/data/datanode
```

**Why it's broken:** The named volumes exist (`nn-data`, `w1-data`, etc.) and are correctly mounted at `/data/...`, but the NameNode/DataNode processes write to `/tmp/minihadoop/...` which is the container's writable layer. Data survives `docker compose stop/start` because the container itself isn't removed, but it is **completely lost** on `docker compose down -v` or container recreation.

**Evidence:** During reproduction:
```
$ docker compose exec worker-1 ls /data/datanode
(empty)

$ docker compose exec worker-1 find / -name 'blk_*'
/tmp/minihadoop/datanode/blk_d708d222-....blk    ← actual location
```

The v2 benchmark script calls `docker compose down -v` between mini-Hadoop and Hadoop phases (in `stop_mini`), which wipes all of `/tmp/minihadoop/` because the containers get recreated from image.

**Fix options (any one works):**
1. Change defaults to `/data/namenode` / `/data/datanode`.
2. Set env vars in `docker-compose.yml`:
   ```yaml
   environment:
     - MINIHADOOP_DATA_DIR=/data/datanode
     - MINIHADOOP_METADATA_DIR=/data/namenode
   ```
3. Change compose mounts to `/tmp/minihadoop/...`.

Option 2 is the smallest diff.

---

## Bug #3 — DataNodes don't re-register after NameNode restart

**Observed:** Immediately after NN stop/start, with 3 worker containers still running and heartbeating:

```
$ curl http://namenode:9100/metrics
{"alive_datanodes":3, ...}         ← reports 3 alive

$ hdfs put /tmp/x /x
Error uploading file: add block: not enough DataNodes: need 3, have 0 alive
                                                              ↑
                                     the block allocator sees 0 alive
```

There's a disagreement inside the NameNode: the metrics count (3) and the block-allocator's view (0) are inconsistent. Likely cause: heartbeats refresh `DataNodeInfo.LastHeartbeat` but don't re-insert the node into `bm.datanodes` if the NN restarted with an empty `datanodes` map. The metrics may be counting something else (e.g. recent heartbeat sources) while `getAliveNodesSorted()` iterates the `datanodes` map and finds nothing.

**Fix sketch:** In the heartbeat RPC handler, if the source nodeID is unknown, auto-register it. Alternatively, force DNs to re-register on any heartbeat-ack-failed signal.

---

## Why v1 tests passed but v2 tests failed

| Test | What it does | Touches bug #1 | Touches #2 | Touches #3 |
|---|---|---|---|---|
| v1 F1 | Kill DN, measure detection time | no | no | no |
| v1 F2 | Kill DN, verify reads | no | no | no |
| v1 F3 | Kill DN, restart DN, wait for block reports | no | on `down -v` only | no |
| v1 F4 | Kill 2 DNs, read (no restart) | no | no | no |
| **v2 F5** | **Restart NN, read** | **yes** | yes | yes |
| **v2 F6** | Restart NN was already done in F5 → then kill-restart DNs, read | yes (inherited) | yes | yes (inherited) |
| v2 F7 | Kill DN during write | no | no | no (DN pipeline separately broken — different bug) |
| v2 F9 | Kill DN, wait, kill another | no | no | no (re-replication not implemented — separate issue) |

**v2 F6 is "caused by" F5.** F5 wipes NN state (bug #1) and loses DN registrations (bug #3). When F6 then tries to write, writes fail silently (suppressed stderr), so later reads produce empty files.

---

## Test-side issues (minor)

1. **stderr suppression** in `scripts/benchmark-v2.sh` line 40:
   ```bash
   mini_exec() { docker compose -f "$MINI_COMPOSE" exec -T client sh -c "$1" 2>/dev/null; }
   ```
   Hid `Error uploading file: ...` messages that would have pointed straight at bug #3. Fix: tee stderr to a log file instead of dropping it.

2. **No exit-code checks on writes**. If `put_fn` fails, the script still runs the read phase. A defensive check would catch these cascading failures earlier.

3. **Assumes clean state between test phases**. A `--fresh-cluster-per-test` flag would have isolated F5 from F6 and produced a different (much cleaner) result set.

**These are quality-of-diagnosis issues — they did not cause the failures.**

---

## Fix priority

| Priority | Fix | Effort | Validates |
|---|---|---|---|
| 🔴 P0 | Wire `LoadState` + edits.log replay into NN startup (bug #1) | ~50 LOC | F5 passes |
| 🔴 P0 | Env-override data paths in docker-compose.yml (bug #2) | 2 lines per service | persistence survives `down -v` |
| 🔴 P0 | Auto-register DN on first heartbeat from unknown nodeID (bug #3) | ~15 LOC | F6 passes, "0 alive" bug gone |
| 🟡 P1 | Fix stderr suppression + add put-rc checks in v2 script | ~10 lines | future diagnosis is one-shot |
| 🟡 P1 | Implement pipeline recovery in DFS client (root cause of F7) | ~100 LOC | F7 passes |
| 🟢 P2 | Implement re-replication (root cause of F9) | larger | F9 passes |

After P0 fixes, I'd expect:
- F5: 5/5 SHA match (bug #1 fix)
- F6: 5/5 SHA match (bug #1+#3 fix)
- F7: still fails (separate pipeline-recovery gap)
- F9: still fails (separate re-replication gap)
- Raw write throughput: unchanged (~26 MB/s at 500MB)

---

## Verdict

> "Whether there's issues with the test, or test setup" — **minor** (stderr suppression slowed diagnosis by ~30 min).
> "Check relevant code in mini-Hadoop" — **three confirmed code bugs** (persistence not loaded, paths mismatched, DN re-registration missing).

The reviewer was right that v2 asked harder questions. The v1 tests never stressed NameNode persistence or DN re-registration, so they didn't surface these bugs. v2 did, and the report should flag them as real work items, not test artifacts.
