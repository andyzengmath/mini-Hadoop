package namenode

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// FSNode represents a file or directory in the namespace.
type FSNode struct {
	Name          string
	IsDir         bool
	Children      map[string]*FSNode // dir only
	Parent        *FSNode
	BlockIDs      []string // file only
	Size          int64    // file only
	Replication   int32    // file only
	Permissions   int32
	ModTime       time.Time
	IsBeingWritten bool   // true during file creation, before CompleteFile
}

// Namespace manages the filesystem directory tree.
type Namespace struct {
	mu   sync.RWMutex
	root *FSNode
}

// NewNamespace creates a new empty namespace with a root directory.
func NewNamespace() *Namespace {
	return &Namespace{
		root: &FSNode{
			Name:     "/",
			IsDir:    true,
			Children: make(map[string]*FSNode),
			ModTime:  time.Now(),
		},
	}
}

// MkDir creates a directory at the given path. If createParents is true,
// it creates intermediate directories as needed.
func (ns *Namespace) MkDir(path string, createParents bool) error {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	parts := splitPath(path)
	if len(parts) == 0 {
		return nil // root always exists
	}

	current := ns.root
	for i, part := range parts {
		child, exists := current.Children[part]
		if exists {
			if !child.IsDir {
				return fmt.Errorf("path component %q is a file, not a directory", joinPath(parts[:i+1]))
			}
			current = child
			continue
		}
		if !createParents && i < len(parts)-1 {
			return fmt.Errorf("parent directory %q does not exist", joinPath(parts[:i+1]))
		}
		newDir := &FSNode{
			Name:     part,
			IsDir:    true,
			Children: make(map[string]*FSNode),
			Parent:   current,
			ModTime:  time.Now(),
		}
		current.Children[part] = newDir
		current = newDir
	}
	return nil
}

// CreateFile creates a new file entry in the namespace.
// The file starts in "being written" state until CompleteFile is called.
func (ns *Namespace) CreateFile(path string, replication int32) error {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	parts := splitPath(path)
	if len(parts) == 0 {
		return fmt.Errorf("cannot create file at root")
	}

	dirParts := parts[:len(parts)-1]
	fileName := parts[len(parts)-1]

	parent, err := ns.lookupDir(dirParts)
	if err != nil {
		return fmt.Errorf("parent directory: %w", err)
	}

	if _, exists := parent.Children[fileName]; exists {
		return fmt.Errorf("file %q already exists", path)
	}

	parent.Children[fileName] = &FSNode{
		Name:           fileName,
		IsDir:          false,
		Parent:         parent,
		BlockIDs:       make([]string, 0),
		Replication:    replication,
		ModTime:        time.Now(),
		IsBeingWritten: true,
	}
	return nil
}

// CompleteFile marks a file as fully written and sets its block list.
func (ns *Namespace) CompleteFile(path string, blockIDs []string, size int64) error {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	node, err := ns.lookup(splitPath(path))
	if err != nil {
		return err
	}
	if node.IsDir {
		return fmt.Errorf("%q is a directory", path)
	}
	node.BlockIDs = blockIDs
	node.Size = size
	node.IsBeingWritten = false
	node.ModTime = time.Now()
	return nil
}

// GetFile returns the FSNode for a file path.
func (ns *Namespace) GetFile(path string) (*FSNode, error) {
	ns.mu.RLock()
	defer ns.mu.RUnlock()

	node, err := ns.lookup(splitPath(path))
	if err != nil {
		return nil, err
	}
	return node, nil
}

// ListDir returns the children of a directory.
func (ns *Namespace) ListDir(path string) ([]*FSNode, error) {
	ns.mu.RLock()
	defer ns.mu.RUnlock()

	parts := splitPath(path)
	var node *FSNode
	var err error
	if len(parts) == 0 {
		node = ns.root
	} else {
		node, err = ns.lookup(parts)
		if err != nil {
			return nil, err
		}
	}

	if !node.IsDir {
		return nil, fmt.Errorf("%q is not a directory", path)
	}

	result := make([]*FSNode, 0, len(node.Children))
	for _, child := range node.Children {
		result = append(result, child)
	}
	return result, nil
}

// Delete removes a file or directory. If recursive is true, removes non-empty directories.
// Returns the list of block IDs that were associated with deleted files.
func (ns *Namespace) Delete(path string, recursive bool) ([]string, error) {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	parts := splitPath(path)
	if len(parts) == 0 {
		return nil, fmt.Errorf("cannot delete root")
	}

	node, err := ns.lookup(parts)
	if err != nil {
		return nil, err
	}

	if node.IsDir && len(node.Children) > 0 && !recursive {
		return nil, fmt.Errorf("directory %q is not empty", path)
	}

	var blockIDs []string
	collectBlocks(node, &blockIDs)

	parent := node.Parent
	delete(parent.Children, node.Name)
	return blockIDs, nil
}

// AddBlockToFile appends a block ID to a file being written.
func (ns *Namespace) AddBlockToFile(path string, blockID string) error {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	node, err := ns.lookup(splitPath(path))
	if err != nil {
		return err
	}
	if node.IsDir {
		return fmt.Errorf("%q is a directory", path)
	}
	if !node.IsBeingWritten {
		return fmt.Errorf("%q is not open for writing", path)
	}
	node.BlockIDs = append(node.BlockIDs, blockID)
	return nil
}

// lookup finds a node by path parts. Caller must hold at least a read lock.
func (ns *Namespace) lookup(parts []string) (*FSNode, error) {
	current := ns.root
	for _, part := range parts {
		if !current.IsDir {
			return nil, fmt.Errorf("%q is not a directory", current.Name)
		}
		child, exists := current.Children[part]
		if !exists {
			return nil, fmt.Errorf("path not found: %q", part)
		}
		current = child
	}
	return current, nil
}

// lookupDir finds a directory node by path parts. Caller must hold at least a read lock.
func (ns *Namespace) lookupDir(parts []string) (*FSNode, error) {
	node, err := ns.lookup(parts)
	if err != nil {
		return nil, err
	}
	if !node.IsDir {
		return nil, fmt.Errorf("%q is not a directory", joinPath(parts))
	}
	return node, nil
}

// collectBlocks recursively collects all block IDs under a node.
func collectBlocks(node *FSNode, blockIDs *[]string) {
	if !node.IsDir {
		*blockIDs = append(*blockIDs, node.BlockIDs...)
		return
	}
	for _, child := range node.Children {
		collectBlocks(child, blockIDs)
	}
}

// splitPath splits a path into components, ignoring leading/trailing slashes.
func splitPath(path string) []string {
	path = strings.Trim(path, "/")
	if path == "" {
		return nil
	}
	return strings.Split(path, "/")
}

// joinPath joins path components with slashes.
func joinPath(parts []string) string {
	return "/" + strings.Join(parts, "/")
}
