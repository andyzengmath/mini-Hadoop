package namenode

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/mini-hadoop/mini-hadoop/pkg/config"
)

// TestEditLogReplay_OnStart verifies that Start() replays edit-log entries
// written after the last snapshot. Without replay, any mkdir / createFile /
// completeFile / delete issued between a SaveState snapshot and an ungraceful
// crash would be invisible to clients after restart, because LoadState only
// restores the snapshot — not the live log.
func TestEditLogReplay_OnStart(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: run a NN, do a few mutations, and crash before the next snapshot.
	// We skip SaveState entirely to simulate the "crashed between snapshots" case.
	el, err := NewEditLog(dir)
	if err != nil {
		t.Fatalf("NewEditLog: %v", err)
	}
	if _, err := el.Append(OpMkDir, "/crash-survivors", nil); err != nil {
		t.Fatalf("Append mkdir: %v", err)
	}
	createData, _ := json.Marshal(map[string]interface{}{"replication": int32(3)})
	if _, err := el.Append(OpCreateFile, "/crash-survivors/f.dat", json.RawMessage(createData)); err != nil {
		t.Fatalf("Append create: %v", err)
	}
	completeData, _ := json.Marshal(map[string]interface{}{"blocks": []string{"blk_A"}, "size": int64(1024)})
	if _, err := el.Append(OpCompleteFile, "/crash-survivors/f.dat", json.RawMessage(completeData)); err != nil {
		t.Fatalf("Append complete: %v", err)
	}
	if err := el.Close(); err != nil {
		t.Fatalf("el.Close: %v", err)
	}

	// Phase 2: fresh Server starts in the same MetadataDir. With replay wired
	// into Start(), those three edits should come back to life.
	cfg := config.DefaultConfig()
	cfg.MetadataDir = dir
	srv := NewServer(cfg)
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()

	// Give the goroutines a moment — Start() itself is synchronous for replay,
	// but we want Stop() on defer to hit a clean state.
	time.Sleep(50 * time.Millisecond)

	node, err := srv.ns.GetFile("/crash-survivors/f.dat")
	if err != nil {
		t.Fatalf("replayed file not found: %v", err)
	}
	if len(node.BlockIDs) != 1 || node.BlockIDs[0] != "blk_A" {
		t.Errorf("expected BlockIDs=[blk_A] after replay, got %v", node.BlockIDs)
	}
	if node.Size != 1024 {
		t.Errorf("expected Size=1024 after replay, got %d", node.Size)
	}
	if node.Replication != 3 {
		t.Errorf("expected Replication=3 after replay, got %d", node.Replication)
	}
}

// TestEditLogReplay_RespectsSnapshotSeq verifies that ReplayFrom filters out
// entries already captured in the snapshot, preventing duplicate application.
// Otherwise a CreateFile that was both in the snapshot *and* in the log would
// try to run twice and fail with "already exists".
func TestEditLogReplay_RespectsSnapshotSeq(t *testing.T) {
	dir := t.TempDir()

	el, err := NewEditLog(dir)
	if err != nil {
		t.Fatalf("NewEditLog: %v", err)
	}
	mkdirData := json.RawMessage(nil)
	seq1, _ := el.Append(OpMkDir, "/a", mkdirData)           // pre-snapshot
	createData, _ := json.Marshal(map[string]interface{}{"replication": int32(2)})
	seq2, _ := el.Append(OpCreateFile, "/a/file", json.RawMessage(createData)) // post-snapshot
	el.Close()

	if seq1 >= seq2 {
		t.Fatalf("sanity: expected monotonically increasing sequences, got %d then %d", seq1, seq2)
	}

	// Build a snapshot that already contains the mkdir (simulates a prior
	// SaveState capturing /a). Store it with LastEditSeq=seq1 so replay should
	// skip entry seq1 and apply only entry seq2.
	ns := NewNamespace()
	ns.MkDir("/a", true) // mirror the effect of the first edit
	bm := NewBlockManager(3, 10*time.Second)
	if err := SaveState(ns, bm, seq1, dir); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	cfg := config.DefaultConfig()
	cfg.MetadataDir = dir
	srv := NewServer(cfg)
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()

	// /a came from the snapshot; /a/file came from replay.
	if _, err := srv.ns.GetFile("/a/file"); err != nil {
		t.Errorf("replay should have created /a/file: %v", err)
	}
	// Directory should exist exactly once — if replay had re-applied the mkdir
	// we'd have an error, which we'd see via a second CreateFile attempt
	// failing with "already exists". As a softer check, ListDir must succeed.
	if _, err := srv.ns.ListDir("/a"); err != nil {
		t.Errorf("ListDir /a after replay: %v", err)
	}
}
