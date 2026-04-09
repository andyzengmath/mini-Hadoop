package namenode

import (
	"testing"
	"time"
)

func TestSaveAndLoadState_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	ns := NewNamespace()
	ns.MkDir("/user/data", true)
	ns.CreateFile("/user/data/file.txt", 3)
	ns.AddBlockToFile("/user/data/file.txt", "blk_001")
	ns.AddBlockToFile("/user/data/file.txt", "blk_002")
	ns.CompleteFile("/user/data/file.txt", []string{"blk_001", "blk_002"}, 2048)

	bm := NewBlockManager(3, 10*time.Second)
	bm.RegisterDataNode("n1", "host1:9001", 100<<30)

	if err := SaveState(ns, bm, dir); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	state, err := LoadState(dir)
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if state == nil {
		t.Fatal("LoadState returned nil")
	}

	restoredNS := RestoreNamespace(state.Namespace)
	node, err := restoredNS.GetFile("/user/data/file.txt")
	if err != nil {
		t.Fatalf("GetFile after restore: %v", err)
	}
	if len(node.BlockIDs) != 2 {
		t.Errorf("expected 2 blocks after restore, got %d", len(node.BlockIDs))
	}
	if node.Size != 2048 {
		t.Errorf("expected Size=2048, got %d", node.Size)
	}
	if node.Replication != 3 {
		t.Errorf("expected Replication=3, got %d", node.Replication)
	}
}

func TestLoadState_NonExistent(t *testing.T) {
	state, err := LoadState(t.TempDir())
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if state != nil {
		t.Fatal("expected nil for non-existent state")
	}
}

func TestRestoreNamespace_Nil(t *testing.T) {
	ns := RestoreNamespace(nil)
	if ns == nil {
		t.Fatal("RestoreNamespace(nil) returned nil")
	}
	children, err := ns.ListDir("/")
	if err != nil {
		t.Fatalf("ListDir on nil-restored namespace: %v", err)
	}
	if len(children) != 0 {
		t.Errorf("expected empty namespace, got %d children", len(children))
	}
}

func TestSaveState_DirectoryStructure(t *testing.T) {
	dir := t.TempDir()
	ns := NewNamespace()
	ns.MkDir("/a/b/c", true)
	ns.CreateFile("/a/file1", 2)
	ns.CompleteFile("/a/file1", []string{"blk_x"}, 512)

	bm := NewBlockManager(2, 10*time.Second)
	if err := SaveState(ns, bm, dir); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	state, _ := LoadState(dir)
	restoredNS := RestoreNamespace(state.Namespace)

	// Verify directory structure preserved
	children, err := restoredNS.ListDir("/a")
	if err != nil {
		t.Fatalf("ListDir /a: %v", err)
	}
	if len(children) != 2 { // "b" dir and "file1" file
		t.Errorf("expected 2 children in /a, got %d", len(children))
	}
}
