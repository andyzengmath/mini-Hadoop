package datanode

import (
	"bytes"
	"os"
	"testing"
)

func newTestStorage(t *testing.T) *BlockStorage {
	t.Helper()
	dir := t.TempDir()
	bs, err := NewBlockStorage(dir)
	if err != nil {
		t.Fatalf("NewBlockStorage: %v", err)
	}
	return bs
}

func TestBlockStorage_WriteReadDelete(t *testing.T) {
	bs := newTestStorage(t)
	data := []byte("hello block storage")

	written, checksum, err := bs.WriteBlock("blk_test001", 1, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("WriteBlock: %v", err)
	}
	if written != int64(len(data)) {
		t.Errorf("written=%d, want %d", written, len(data))
	}
	if len(checksum) == 0 {
		t.Error("expected non-empty checksum")
	}

	if !bs.HasBlock("blk_test001") {
		t.Error("HasBlock returned false after write")
	}

	reader, size, err := bs.ReadBlock("blk_test001")
	if err != nil {
		t.Fatalf("ReadBlock: %v", err)
	}
	if size != int64(len(data)) {
		t.Errorf("ReadBlock size=%d, want %d", size, len(data))
	}

	var buf bytes.Buffer
	buf.ReadFrom(reader)
	reader.Close() // Must close before delete on Windows (file locking)
	if !bytes.Equal(buf.Bytes(), data) {
		t.Error("read data doesn't match written data")
	}

	if err := bs.DeleteBlock("blk_test001"); err != nil {
		t.Fatalf("DeleteBlock: %v", err)
	}
	if bs.HasBlock("blk_test001") {
		t.Error("HasBlock returned true after delete")
	}
}

func TestBlockStorage_PathTraversalRejected(t *testing.T) {
	bs := newTestStorage(t)
	badIDs := []string{"../escape", "..\\escape", "a/b", "a\\b", ""}
	for _, id := range badIDs {
		_, _, err := bs.WriteBlock(id, 1, bytes.NewReader([]byte("data")))
		if err == nil {
			t.Errorf("WriteBlock(%q): expected path traversal error", id)
		}
	}
}

func TestBlockStorage_ReadNonExistent(t *testing.T) {
	bs := newTestStorage(t)
	_, _, err := bs.ReadBlock("blk_nothere")
	if err == nil {
		t.Error("expected error reading nonexistent block")
	}
}

func TestBlockStorage_DeleteNonExistent(t *testing.T) {
	bs := newTestStorage(t)
	// Should not error — treat as already gone
	if err := bs.DeleteBlock("blk_nothere"); err != nil {
		t.Errorf("DeleteBlock non-existent should not error: %v", err)
	}
}

func TestBlockStorage_DeletePathTraversal(t *testing.T) {
	bs := newTestStorage(t)
	err := bs.DeleteBlock("../../etc/passwd")
	if err == nil {
		t.Error("expected error for path traversal in DeleteBlock")
	}
}

func TestBlockStorage_ReadPathTraversal(t *testing.T) {
	bs := newTestStorage(t)
	_, _, err := bs.ReadBlock("../../../etc/shadow")
	if err == nil {
		t.Error("expected error for path traversal in ReadBlock")
	}
}

func TestBlockStorage_GetBlockReport(t *testing.T) {
	bs := newTestStorage(t)
	bs.WriteBlock("blk_a", 1, bytes.NewReader([]byte("aaa")))
	bs.WriteBlock("blk_b", 1, bytes.NewReader([]byte("bbb")))

	report := bs.GetBlockReport()
	if len(report) != 2 {
		t.Errorf("expected 2 blocks in report, got %d", len(report))
	}
}

func TestBlockStorage_GetUsedBytes(t *testing.T) {
	bs := newTestStorage(t)
	bs.WriteBlock("blk_a", 1, bytes.NewReader([]byte("12345")))
	bs.WriteBlock("blk_b", 1, bytes.NewReader([]byte("67890")))

	used := bs.GetUsedBytes()
	if used != 10 {
		t.Errorf("expected 10 used bytes, got %d", used)
	}
}

func TestBlockStorage_BlockCount(t *testing.T) {
	bs := newTestStorage(t)
	if bs.BlockCount() != 0 {
		t.Error("expected 0 blocks initially")
	}
	bs.WriteBlock("blk_1", 1, bytes.NewReader([]byte("data")))
	if bs.BlockCount() != 1 {
		t.Errorf("expected 1 block, got %d", bs.BlockCount())
	}
}

func TestBlockStorage_ScanOnStartup(t *testing.T) {
	dir := t.TempDir()
	// Pre-populate with a .blk file
	os.WriteFile(dir+"/blk_existing.blk", []byte("preexisting data"), 0644)

	bs, err := NewBlockStorage(dir)
	if err != nil {
		t.Fatalf("NewBlockStorage: %v", err)
	}
	if !bs.HasBlock("blk_existing") {
		t.Error("expected pre-existing block to be scanned on startup")
	}
	if bs.BlockCount() != 1 {
		t.Errorf("expected 1 block from scan, got %d", bs.BlockCount())
	}
}
