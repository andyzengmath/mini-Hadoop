package namenode

import (
	"testing"
)

func TestEditLog_AppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	el, err := NewEditLog(dir)
	if err != nil {
		t.Fatalf("NewEditLog: %v", err)
	}

	// Append some entries
	seq1, err := el.Append(OpMkDir, "/data", nil)
	if err != nil {
		t.Fatalf("Append mkdir: %v", err)
	}
	if seq1 != 1 {
		t.Errorf("expected seq 1, got %d", seq1)
	}

	type createData struct {
		Replication int32 `json:"replication"`
	}
	seq2, err := el.Append(OpCreateFile, "/data/file.txt", createData{Replication: 3})
	if err != nil {
		t.Fatalf("Append create: %v", err)
	}
	if seq2 != 2 {
		t.Errorf("expected seq 2, got %d", seq2)
	}

	seq3, _ := el.Append(OpAddBlock, "/data/file.txt", map[string]string{"block_id": "blk_001"})
	if seq3 != 3 {
		t.Errorf("expected seq 3, got %d", seq3)
	}

	el.Close()

	// Replay all entries
	el2, err := NewEditLog(dir)
	if err != nil {
		t.Fatalf("reopen EditLog: %v", err)
	}
	defer el2.Close()

	// Sequence should resume from last
	if el2.GetSequence() != 3 {
		t.Errorf("expected sequence 3 after reopen, got %d", el2.GetSequence())
	}

	var replayed []EditEntry
	count, err := el2.Replay(func(entry EditEntry) error {
		replayed = append(replayed, entry)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 replayed, got %d", count)
	}

	if replayed[0].Operation != OpMkDir || replayed[0].Path != "/data" {
		t.Errorf("entry 0: expected MKDIR /data, got %s %s", replayed[0].Operation, replayed[0].Path)
	}
	if replayed[1].Operation != OpCreateFile {
		t.Errorf("entry 1: expected CREATE_FILE, got %s", replayed[1].Operation)
	}
	if replayed[2].Operation != OpAddBlock {
		t.Errorf("entry 2: expected ADD_BLOCK, got %s", replayed[2].Operation)
	}
}

func TestEditLog_ReplayFrom(t *testing.T) {
	dir := t.TempDir()
	el, _ := NewEditLog(dir)

	el.Append(OpMkDir, "/a", nil)
	el.Append(OpMkDir, "/b", nil)
	el.Append(OpMkDir, "/c", nil)
	el.Close()

	el2, _ := NewEditLog(dir)
	defer el2.Close()

	var entries []EditEntry
	count, err := el2.ReplayFrom(1, func(entry EditEntry) error {
		entries = append(entries, entry)
		return nil
	})
	if err != nil {
		t.Fatalf("ReplayFrom: %v", err)
	}
	if count != 2 { // seq 2 and 3 (skipping seq 1)
		t.Errorf("expected 2 entries from seq>1, got %d", count)
	}
	if entries[0].Path != "/b" {
		t.Errorf("expected /b first, got %s", entries[0].Path)
	}
}

func TestEditLog_Truncate(t *testing.T) {
	dir := t.TempDir()
	el, _ := NewEditLog(dir)

	el.Append(OpMkDir, "/old", nil)
	el.Append(OpMkDir, "/old2", nil)

	if err := el.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	// New entries should continue from the same sequence
	seq, _ := el.Append(OpMkDir, "/new", nil)
	if seq != 3 {
		t.Errorf("expected seq 3 after truncate, got %d", seq)
	}
	el.Close()

	// Replay should only see the post-truncate entry
	el2, _ := NewEditLog(dir)
	defer el2.Close()

	var entries []EditEntry
	el2.Replay(func(entry EditEntry) error {
		entries = append(entries, entry)
		return nil
	})
	if len(entries) != 1 {
		t.Errorf("expected 1 entry after truncate, got %d", len(entries))
	}
	if len(entries) > 0 && entries[0].Path != "/new" {
		t.Errorf("expected /new, got %s", entries[0].Path)
	}
}

func TestEditLog_EmptyReplay(t *testing.T) {
	dir := t.TempDir()
	el, _ := NewEditLog(dir)
	defer el.Close()

	count, err := el.Replay(func(entry EditEntry) error {
		t.Error("should not be called on empty log")
		return nil
	})
	if err != nil {
		t.Fatalf("Replay empty: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 replayed, got %d", count)
	}
}

func TestEditLog_SequenceResumesAfterReopen(t *testing.T) {
	dir := t.TempDir()

	el, _ := NewEditLog(dir)
	el.Append(OpMkDir, "/a", nil)
	el.Append(OpMkDir, "/b", nil)
	el.Close()

	el2, _ := NewEditLog(dir)
	seq, _ := el2.Append(OpMkDir, "/c", nil)
	el2.Close()

	if seq != 3 {
		t.Errorf("expected seq 3 on second open, got %d", seq)
	}
}
