package namenode

import (
	"testing"
)

func TestMkDir_CreateParents(t *testing.T) {
	ns := NewNamespace()
	if err := ns.MkDir("/a/b/c", true); err != nil {
		t.Fatalf("MkDir with createParents: %v", err)
	}
	children, err := ns.ListDir("/a/b")
	if err != nil {
		t.Fatalf("ListDir: %v", err)
	}
	if len(children) != 1 || children[0].Name != "c" {
		t.Errorf("expected child 'c', got %v", children)
	}
}

func TestMkDir_NoCreateParents_Error(t *testing.T) {
	ns := NewNamespace()
	if err := ns.MkDir("/missing/dir", false); err == nil {
		t.Fatal("expected error creating dir without parents")
	}
}

func TestMkDir_Root(t *testing.T) {
	ns := NewNamespace()
	if err := ns.MkDir("/", true); err != nil {
		t.Fatalf("MkDir on root should succeed: %v", err)
	}
}

func TestMkDir_PathComponentIsFile(t *testing.T) {
	ns := NewNamespace()
	ns.MkDir("/d", true)
	ns.CreateFile("/d/f", 3)
	if err := ns.MkDir("/d/f/sub", true); err == nil {
		t.Fatal("expected error when path component is a file")
	}
}

func TestCreateFile_Success(t *testing.T) {
	ns := NewNamespace()
	ns.MkDir("/data", true)
	if err := ns.CreateFile("/data/file.txt", 3); err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	node, err := ns.GetFile("/data/file.txt")
	if err != nil {
		t.Fatalf("GetFile: %v", err)
	}
	if node.IsDir {
		t.Error("expected file, got directory")
	}
	if !node.IsBeingWritten {
		t.Error("expected IsBeingWritten=true before CompleteFile")
	}
	if node.Replication != 3 {
		t.Errorf("Replication = %d, want 3", node.Replication)
	}
}

func TestCreateFile_Duplicate(t *testing.T) {
	ns := NewNamespace()
	ns.MkDir("/d", true)
	ns.CreateFile("/d/f", 3)
	if err := ns.CreateFile("/d/f", 3); err == nil {
		t.Fatal("expected error creating duplicate file")
	}
}

func TestCreateFile_MissingParent(t *testing.T) {
	ns := NewNamespace()
	if err := ns.CreateFile("/missing/file.txt", 3); err == nil {
		t.Fatal("expected error creating file with missing parent")
	}
}

func TestCreateFile_AtRoot(t *testing.T) {
	ns := NewNamespace()
	if err := ns.CreateFile("/", 3); err == nil {
		t.Fatal("expected error creating file at root")
	}
}

func TestCompleteFile_SetsState(t *testing.T) {
	ns := NewNamespace()
	ns.MkDir("/d", true)
	ns.CreateFile("/d/f", 3)
	ns.AddBlockToFile("/d/f", "blk_001")
	if err := ns.CompleteFile("/d/f", []string{"blk_001"}, 1024); err != nil {
		t.Fatalf("CompleteFile: %v", err)
	}
	node, _ := ns.GetFile("/d/f")
	if node.IsBeingWritten {
		t.Error("expected IsBeingWritten=false after CompleteFile")
	}
	if node.Size != 1024 {
		t.Errorf("Size = %d, want 1024", node.Size)
	}
	if len(node.BlockIDs) != 1 || node.BlockIDs[0] != "blk_001" {
		t.Errorf("BlockIDs = %v, want [blk_001]", node.BlockIDs)
	}
}

func TestAddBlockToFile_NotWriting(t *testing.T) {
	ns := NewNamespace()
	ns.MkDir("/d", true)
	ns.CreateFile("/d/f", 1)
	ns.CompleteFile("/d/f", nil, 0)
	if err := ns.AddBlockToFile("/d/f", "blk_new"); err == nil {
		t.Fatal("expected error adding block to completed file")
	}
}

func TestDelete_File(t *testing.T) {
	ns := NewNamespace()
	ns.MkDir("/d", true)
	ns.CreateFile("/d/f", 1)
	ns.CompleteFile("/d/f", []string{"blk_a", "blk_b"}, 2048)
	blockIDs, err := ns.Delete("/d/f", false)
	if err != nil {
		t.Fatalf("Delete file: %v", err)
	}
	if len(blockIDs) != 2 {
		t.Errorf("expected 2 block IDs, got %d: %v", len(blockIDs), blockIDs)
	}
}

func TestDelete_NonEmptyDir_NonRecursive(t *testing.T) {
	ns := NewNamespace()
	ns.MkDir("/dir", true)
	ns.CreateFile("/dir/file", 1)
	_, err := ns.Delete("/dir", false)
	if err == nil {
		t.Fatal("expected error deleting non-empty dir without recursive")
	}
}

func TestDelete_Recursive_CollectsBlocks(t *testing.T) {
	ns := NewNamespace()
	ns.MkDir("/dir/sub", true)
	ns.CreateFile("/dir/f1", 1)
	ns.CompleteFile("/dir/f1", []string{"blk_a"}, 100)
	ns.CreateFile("/dir/sub/f2", 1)
	ns.CompleteFile("/dir/sub/f2", []string{"blk_b", "blk_c"}, 200)

	blockIDs, err := ns.Delete("/dir", true)
	if err != nil {
		t.Fatalf("Delete recursive: %v", err)
	}
	if len(blockIDs) != 3 {
		t.Errorf("expected 3 block IDs, got %d: %v", len(blockIDs), blockIDs)
	}
}

func TestDelete_Root(t *testing.T) {
	ns := NewNamespace()
	_, err := ns.Delete("/", false)
	if err == nil {
		t.Fatal("expected error deleting root")
	}
}

func TestListDir_Empty(t *testing.T) {
	ns := NewNamespace()
	children, err := ns.ListDir("/")
	if err != nil {
		t.Fatalf("ListDir root: %v", err)
	}
	if len(children) != 0 {
		t.Errorf("expected empty root, got %d children", len(children))
	}
}

func TestListDir_NotADirectory(t *testing.T) {
	ns := NewNamespace()
	ns.MkDir("/d", true)
	ns.CreateFile("/d/f", 1)
	_, err := ns.ListDir("/d/f")
	if err == nil {
		t.Fatal("expected error listing a file as directory")
	}
}

func TestGetFile_NotFound(t *testing.T) {
	ns := NewNamespace()
	_, err := ns.GetFile("/nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
}

func TestSplitPath(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"/", 0},
		{"/a", 1},
		{"/a/b/c", 3},
		{"a/b", 2},
		{"", 0},
	}
	for _, tt := range tests {
		parts := splitPath(tt.input)
		if len(parts) != tt.want {
			t.Errorf("splitPath(%q) = %d parts, want %d", tt.input, len(parts), tt.want)
		}
	}
}
