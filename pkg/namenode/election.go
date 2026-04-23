package namenode

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Role represents the NameNode's current HA role.
type Role string

const (
	RoleActive  Role = "ACTIVE"
	RoleStandby Role = "STANDBY"
)

// LeaderElection manages HA leadership state for NameNode.
// In production, this would use ZooKeeper. This implementation provides
// the interface and state management, with a pluggable backend.
type LeaderElection struct {
	mu         sync.RWMutex
	nodeID     string
	role       Role
	leaderID   string
	fenceToken int64
	onElected  func() // Called when this node becomes leader
	onLost     func() // Called when leadership is lost
	backend    ElectionBackend
}

// ElectionBackend abstracts the leader election mechanism.
// Implementations: ZooKeeperBackend (production), LocalBackend (single-node).
type ElectionBackend interface {
	// Campaign starts participating in the election.
	Campaign(nodeID string) error
	// Resign gives up leadership.
	Resign() error
	// GetLeader returns the current leader's node ID.
	GetLeader() (string, error)
	// IsLeader returns true if this node is the leader.
	IsLeader() bool
	// Watch blocks until leadership changes, then returns.
	Watch() error
	// Close releases resources.
	Close() error
}

// NewLeaderElection creates a new HA election manager.
func NewLeaderElection(nodeID string, backend ElectionBackend, onElected, onLost func()) *LeaderElection {
	return &LeaderElection{
		nodeID:    nodeID,
		role:      RoleStandby,
		backend:   backend,
		onElected: onElected,
		onLost:    onLost,
	}
}

// Start begins participating in the leader election.
func (le *LeaderElection) Start() error {
	if err := le.backend.Campaign(le.nodeID); err != nil {
		return fmt.Errorf("campaign: %w", err)
	}

	go le.electionLoop()
	return nil
}

// GetRole returns the current HA role.
func (le *LeaderElection) GetRole() Role {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.role
}

// GetFenceToken returns the current fencing token.
func (le *LeaderElection) GetFenceToken() int64 {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.fenceToken
}

// IsActive returns true if this node is the active leader.
func (le *LeaderElection) IsActive() bool {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.role == RoleActive
}

func (le *LeaderElection) electionLoop() {
	for {
		if le.backend.IsLeader() {
			le.becomeActive()
		} else {
			le.becomeStandby()
		}

		// Watch for leadership changes
		if err := le.backend.Watch(); err != nil {
			slog.Warn("election watch error", "error", err)
			time.Sleep(1 * time.Second)
		}
	}
}

func (le *LeaderElection) becomeActive() {
	le.mu.Lock()
	if le.role == RoleActive {
		le.mu.Unlock()
		return // Already active
	}
	le.role = RoleActive
	le.fenceToken++
	le.leaderID = le.nodeID
	le.mu.Unlock()

	slog.Info("became ACTIVE leader",
		"nodeID", le.nodeID,
		"fenceToken", le.fenceToken,
	)

	if le.onElected != nil {
		le.onElected()
	}
}

func (le *LeaderElection) becomeStandby() {
	le.mu.Lock()
	wasActive := le.role == RoleActive
	le.role = RoleStandby
	le.mu.Unlock()

	if wasActive {
		slog.Warn("lost leadership, becoming STANDBY", "nodeID", le.nodeID)
		if le.onLost != nil {
			le.onLost()
		}
	}
}

// Stop resigns from the election.
func (le *LeaderElection) Stop() error {
	return le.backend.Resign()
}

// --- Local Backend (single-node, always leader) ---

// LocalBackend is a single-node election backend that always wins.
// Used for development and testing without ZooKeeper.
//
// isLeader uses atomic.Bool because Campaign/Resign run on the election
// goroutine spawned by LeaderElection.Start(), while IsLeader() is read
// concurrently by callers on other goroutines (e.g. RPC handlers). A plain
// bool caused the race detector to fire when two servers were started within
// the same test process (observed during editlog-replay test development).
type LocalBackend struct {
	isLeader atomic.Bool
	done     chan struct{}
	doneOnce sync.Once
}

// NewLocalBackend creates a backend that immediately becomes leader.
func NewLocalBackend() *LocalBackend {
	return &LocalBackend{
		done: make(chan struct{}),
	}
}

func (b *LocalBackend) Campaign(nodeID string) error {
	b.isLeader.Store(true)
	slog.Info("local election: immediately became leader", "nodeID", nodeID)
	return nil
}

func (b *LocalBackend) Resign() error {
	b.isLeader.Store(false)
	// close(done) must run at most once; Close() delegates to Resign() so the
	// same backend can see multiple resigns during shutdown. sync.Once makes
	// that idempotent.
	b.doneOnce.Do(func() { close(b.done) })
	return nil
}

func (b *LocalBackend) GetLeader() (string, error) {
	return "local", nil
}

func (b *LocalBackend) IsLeader() bool {
	return b.isLeader.Load()
}

func (b *LocalBackend) Watch() error {
	<-b.done // Block until resigned
	return nil
}

func (b *LocalBackend) Close() error {
	return b.Resign()
}
