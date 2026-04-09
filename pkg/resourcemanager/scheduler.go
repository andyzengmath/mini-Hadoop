package resourcemanager

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
)

// NodeInfo tracks a registered NodeManager.
type NodeInfo struct {
	NodeID        string
	Address       string
	TotalMemoryMB int32
	TotalCPU      int32
	UsedMemoryMB  int32
	UsedCPU       int32
	LastHeartbeat time.Time
	Alive         bool
	Containers    map[string]*ContainerInfo // containerID -> info
}

// AvailableMemory returns free memory on this node.
func (n *NodeInfo) AvailableMemory() int32 { return n.TotalMemoryMB - n.UsedMemoryMB }

// AvailableCPU returns free CPU on this node.
func (n *NodeInfo) AvailableCPU() int32 { return n.TotalCPU - n.UsedCPU }

// ContainerInfo tracks a running container.
type ContainerInfo struct {
	ContainerID string
	AppID       string
	NodeID      string
	MemoryMB    int32
	CPUVCores   int32
	Command     string
	Args        []string
	Env         map[string]string
	State       string // RUNNING, COMPLETED, FAILED, KILLED
	ExitCode    int32
	Diagnostics string
}

// AppInfo tracks a submitted application.
type AppInfo struct {
	AppID           string
	Type            string
	State           string // SUBMITTED, RUNNING, FINISHED, FAILED, KILLED
	AMBinaryPath    string
	AMArgs          []string
	AMMemoryMB      int32
	AMCPUVCores     int32
	AMEnv           map[string]string
	AMAddress       string
	AMContainerID   string
	Progress        float32
	Diagnostics     string
	StartTime       time.Time
	FinishTime      time.Time
	Containers      []string // container IDs allocated to this app
}

// PendingRequest is a queued container allocation request.
type PendingRequest struct {
	AppID          string
	RequestID      string
	MemoryMB       int32
	CPUVCores      int32
	PreferredNodes []string
	Relaxed        bool
	Count          int32
}

// FIFOScheduler implements a simple FIFO resource scheduler.
type FIFOScheduler struct {
	mu sync.Mutex

	nodes    map[string]*NodeInfo   // nodeID -> info
	apps     map[string]*AppInfo    // appID -> info
	pending  []PendingRequest        // FIFO queue
	deadTimeout time.Duration
}

// NewFIFOScheduler creates a new scheduler.
func NewFIFOScheduler(deadTimeout time.Duration) *FIFOScheduler {
	return &FIFOScheduler{
		nodes:       make(map[string]*NodeInfo),
		apps:        make(map[string]*AppInfo),
		deadTimeout: deadTimeout,
	}
}

// RegisterNode adds a NodeManager to the cluster.
func (s *FIFOScheduler) RegisterNode(nodeID, address string, memoryMB, cpu int32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.nodes[nodeID] = &NodeInfo{
		NodeID:        nodeID,
		Address:       address,
		TotalMemoryMB: memoryMB,
		TotalCPU:      cpu,
		LastHeartbeat: time.Now(),
		Alive:         true,
		Containers:    make(map[string]*ContainerInfo),
	}
	slog.Info("NodeManager registered", "nodeID", nodeID, "address", address,
		"memory_mb", memoryMB, "cpu", cpu)
}

// SubmitApp registers a new application and allocates its AM container.
func (s *FIFOScheduler) SubmitApp(appType, amBinary string, amArgs []string, amMemMB, amCPU int32, amEnv map[string]string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	appID := "app_" + uuid.New().String()[:8]

	app := &AppInfo{
		AppID:        appID,
		Type:         appType,
		State:        "SUBMITTED",
		AMBinaryPath: amBinary,
		AMArgs:       amArgs,
		AMMemoryMB:   amMemMB,
		AMCPUVCores:  amCPU,
		AMEnv:        amEnv,
		StartTime:    time.Now(),
	}
	s.apps[appID] = app

	// Allocate AM container immediately (FIFO, first available node)
	container, err := s.allocateOne(appID, amMemMB, amCPU, nil, true)
	if err != nil {
		app.State = "FAILED"
		app.Diagnostics = "No resources for AM: " + err.Error()
		return appID, err
	}

	container.Command = amBinary
	container.Args = amArgs
	container.Env = amEnv
	app.AMContainerID = container.ContainerID
	app.State = "RUNNING"
	app.Containers = append(app.Containers, container.ContainerID)

	slog.Info("application submitted", "appID", appID, "amContainer", container.ContainerID,
		"amNode", container.NodeID)
	return appID, nil
}

// AllocateContainers processes container requests from an ApplicationMaster.
func (s *FIFOScheduler) AllocateContainers(appID string, requests []PendingRequest) []*ContainerInfo {
	s.mu.Lock()
	defer s.mu.Unlock()

	app, exists := s.apps[appID]
	if !exists || app.State != "RUNNING" {
		return nil
	}

	var allocated []*ContainerInfo

	for _, req := range requests {
		for i := int32(0); i < req.Count; i++ {
			container, err := s.allocateOne(appID, req.MemoryMB, req.CPUVCores, req.PreferredNodes, req.Relaxed)
			if err != nil {
				// Queue remaining as pending for later allocation
				remaining := PendingRequest{
					AppID:          appID,
					RequestID:      req.RequestID,
					MemoryMB:       req.MemoryMB,
					CPUVCores:      req.CPUVCores,
					PreferredNodes: req.PreferredNodes,
					Relaxed:        req.Relaxed,
					Count:          req.Count - i,
				}
				s.pending = append(s.pending, remaining)
				break
			}
			allocated = append(allocated, container)
			app.Containers = append(app.Containers, container.ContainerID)
		}
	}

	return allocated
}

// allocateOne finds a node and creates a container. Caller must hold the lock.
func (s *FIFOScheduler) allocateOne(appID string, memMB, cpu int32, preferredNodes []string, relaxed bool) (*ContainerInfo, error) {
	// Try preferred nodes first
	for _, nodeID := range preferredNodes {
		node, exists := s.nodes[nodeID]
		if !exists || !node.Alive {
			continue
		}
		if node.AvailableMemory() >= memMB && node.AvailableCPU() >= cpu {
			return s.createContainer(appID, node, memMB, cpu), nil
		}
	}

	// Also try matching by address (locality hints may be addresses, not IDs)
	for _, preferred := range preferredNodes {
		for _, node := range s.nodes {
			if node.Address == preferred && node.Alive &&
				node.AvailableMemory() >= memMB && node.AvailableCPU() >= cpu {
				return s.createContainer(appID, node, memMB, cpu), nil
			}
		}
	}

	if !relaxed && len(preferredNodes) > 0 {
		return nil, fmt.Errorf("no preferred nodes available with sufficient resources")
	}

	// Fall back to any available node (FIFO: first fit)
	for _, node := range s.nodes {
		if node.Alive && node.AvailableMemory() >= memMB && node.AvailableCPU() >= cpu {
			return s.createContainer(appID, node, memMB, cpu), nil
		}
	}

	return nil, fmt.Errorf("no nodes available with %dMB memory and %d CPU", memMB, cpu)
}

func (s *FIFOScheduler) createContainer(appID string, node *NodeInfo, memMB, cpu int32) *ContainerInfo {
	containerID := "container_" + uuid.New().String()[:8]

	container := &ContainerInfo{
		ContainerID: containerID,
		AppID:       appID,
		NodeID:      node.NodeID,
		MemoryMB:    memMB,
		CPUVCores:   cpu,
		State:       "RUNNING",
	}

	node.Containers[containerID] = container
	node.UsedMemoryMB += memMB
	node.UsedCPU += cpu

	return container
}

// ReleaseContainers frees containers back to the cluster.
func (s *FIFOScheduler) ReleaseContainers(appID string, containerIDs []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, cid := range containerIDs {
		for _, node := range s.nodes {
			if c, exists := node.Containers[cid]; exists {
				node.UsedMemoryMB -= c.MemoryMB
				node.UsedCPU -= c.CPUVCores
				delete(node.Containers, cid)
				break
			}
		}
	}
}

// UpdateContainerStatus updates a container's state from NM heartbeat.
func (s *FIFOScheduler) UpdateContainerStatus(containerID, nodeID string, state string, exitCode int32, diagnostics string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	node, exists := s.nodes[nodeID]
	if !exists {
		return
	}

	c, exists := node.Containers[containerID]
	if !exists {
		return
	}

	wasRunning := c.State == "RUNNING"
	c.State = state
	c.ExitCode = exitCode
	c.Diagnostics = diagnostics

	// If the AM container completed/failed, update app state
	for _, app := range s.apps {
		if app.AMContainerID == containerID && (state == "COMPLETED" || state == "FAILED") {
			if state == "COMPLETED" {
				app.State = "FINISHED"
			} else {
				app.State = "FAILED"
				app.Diagnostics = diagnostics
			}
			app.FinishTime = time.Now()
		}
	}

	// Release resources only on first transition out of RUNNING (prevent double-free)
	if wasRunning && (state == "COMPLETED" || state == "FAILED" || state == "KILLED") {
		node.UsedMemoryMB -= c.MemoryMB
		node.UsedCPU -= c.CPUVCores
	}
}

// ProcessNodeHeartbeat updates node liveness and processes pending allocations.
func (s *FIFOScheduler) ProcessNodeHeartbeat(nodeID string, status *NodeInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()

	node, exists := s.nodes[nodeID]
	if !exists {
		return
	}

	node.LastHeartbeat = time.Now()
	node.Alive = true
	if status != nil {
		node.UsedMemoryMB = status.UsedMemoryMB
		node.UsedCPU = status.UsedCPU
	}

	// Try to fulfill pending requests
	s.tryFulfillPending()
}

func (s *FIFOScheduler) tryFulfillPending() {
	remaining := make([]PendingRequest, 0)
	for _, req := range s.pending {
		allocated := false
		container, err := s.allocateOne(req.AppID, req.MemoryMB, req.CPUVCores, req.PreferredNodes, req.Relaxed)
		if err == nil {
			app := s.apps[req.AppID]
			if app != nil {
				app.Containers = append(app.Containers, container.ContainerID)
			}
			allocated = true
		}
		if !allocated {
			remaining = append(remaining, req)
		}
	}
	s.pending = remaining
}

// DetectDeadNodes marks nodes as dead if heartbeat timed out.
func (s *FIFOScheduler) DetectDeadNodes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	var dead []string
	now := time.Now()
	for nodeID, node := range s.nodes {
		if node.Alive && now.Sub(node.LastHeartbeat) > s.deadTimeout {
			node.Alive = false
			dead = append(dead, nodeID)
			slog.Warn("NodeManager marked dead", "nodeID", nodeID)
		}
	}
	return dead
}

// GetAppInfo returns application status.
func (s *FIFOScheduler) GetAppInfo(appID string) (*AppInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	app, exists := s.apps[appID]
	if !exists {
		return nil, fmt.Errorf("unknown application: %s", appID)
	}
	copied := *app
	return &copied, nil
}

// SetAppAMAddress sets the AM's contact address.
func (s *FIFOScheduler) SetAppAMAddress(appID, address string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if app, exists := s.apps[appID]; exists {
		app.AMAddress = address
	}
}

// SetAppProgress updates the application progress.
func (s *FIFOScheduler) SetAppProgress(appID string, progress float32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if app, exists := s.apps[appID]; exists {
		app.Progress = progress
	}
}

// CompleteApp marks an application as finished.
func (s *FIFOScheduler) CompleteApp(appID, state, diagnostics string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if app, exists := s.apps[appID]; exists {
		app.State = state
		app.Diagnostics = diagnostics
		app.FinishTime = time.Now()
	}
}

// GetNodeAddress returns the address for a node ID.
func (s *FIFOScheduler) GetNodeAddress(nodeID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if node, exists := s.nodes[nodeID]; exists {
		return node.Address
	}
	return ""
}

// GetContainerInfo returns info about a specific container.
func (s *FIFOScheduler) GetContainerInfo(containerID string) *ContainerInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, node := range s.nodes {
		if c, exists := node.Containers[containerID]; exists {
			copied := *c
			return &copied
		}
	}
	return nil
}
