package nodemanager

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/mini-hadoop/mini-hadoop/pkg/config"
	"github.com/mini-hadoop/mini-hadoop/pkg/rpc"

	pb "github.com/mini-hadoop/mini-hadoop/proto"

	"google.golang.org/grpc"
)

// ContainerProcess tracks a running container's OS process.
type ContainerProcess struct {
	ContainerID string
	AppID       string
	Cmd         *exec.Cmd
	State       string // RUNNING, COMPLETED, FAILED, KILLED
	ExitCode    int32
	Diagnostics string
	MemoryMB    int32
	CPUVCores   int32
}

// Server implements the NodeManagerService gRPC interface.
type Server struct {
	pb.UnimplementedNodeManagerServiceServer

	nodeID  string
	address string
	cfg     config.Config

	mu         sync.Mutex
	containers map[string]*ContainerProcess

	rmConn   *grpc.ClientConn
	rmClient pb.ResourceManagerServiceClient

	stopCh chan struct{}
}

// NewServer creates a new NodeManager server.
func NewServer(nodeID, address string, cfg config.Config) *Server {
	return &Server{
		nodeID:     nodeID,
		address:    address,
		cfg:        cfg,
		containers: make(map[string]*ContainerProcess),
		stopCh:     make(chan struct{}),
	}
}

// Start connects to the ResourceManager and begins background tasks.
func (s *Server) Start() error {
	conn, err := rpc.DialWithRetry(s.cfg.ResourceManagerAddress(), 5)
	if err != nil {
		return fmt.Errorf("connect to ResourceManager: %w", err)
	}
	s.rmConn = conn
	s.rmClient = pb.NewResourceManagerServiceClient(conn)

	// Register with RM
	resp, err := s.rmClient.RegisterNodeManager(context.Background(), &pb.RegisterNodeManagerRequest{
		NodeId:        s.nodeID,
		Address:       s.address,
		TotalMemoryMb: s.cfg.TotalMemoryMB,
		TotalCpu:      s.cfg.TotalCPU,
	})
	if err != nil {
		return fmt.Errorf("register with RM: %w", err)
	}
	if !resp.Success {
		return fmt.Errorf("registration rejected: %s", resp.Error)
	}

	go s.heartbeatLoop()
	slog.Info("NodeManager started", "nodeID", s.nodeID, "address", s.address)
	return nil
}

// Stop halts all containers and background tasks.
func (s *Server) Stop() {
	close(s.stopCh)

	s.mu.Lock()
	for _, cp := range s.containers {
		if cp.Cmd != nil && cp.Cmd.Process != nil && cp.State == "RUNNING" {
			cp.Cmd.Process.Kill()
		}
	}
	s.mu.Unlock()

	if s.rmConn != nil {
		s.rmConn.Close()
	}
	slog.Info("NodeManager stopped", "nodeID", s.nodeID)
}

func (s *Server) heartbeatLoop() {
	ticker := time.NewTicker(s.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.sendHeartbeat()
		case <-s.stopCh:
			return
		}
	}
}

func (s *Server) sendHeartbeat() {
	s.mu.Lock()
	var usedMem, usedCPU int32
	var statuses []*pb.ContainerStatusReport

	for _, cp := range s.containers {
		if cp.State == "RUNNING" {
			usedMem += cp.MemoryMB
			usedCPU += cp.CPUVCores
		}
		// Report all non-running containers to RM
		if cp.State != "RUNNING" {
			state := pb.ContainerState_CONTAINER_STATE_UNSPECIFIED
			switch cp.State {
			case "COMPLETED":
				state = pb.ContainerState_CONTAINER_COMPLETED
			case "FAILED":
				state = pb.ContainerState_CONTAINER_FAILED
			case "KILLED":
				state = pb.ContainerState_CONTAINER_KILLED
			}
			statuses = append(statuses, &pb.ContainerStatusReport{
				ContainerId: cp.ContainerID,
				State:       state,
				ExitCode:    cp.ExitCode,
				Diagnostics: cp.Diagnostics,
			})
		}
	}
	s.mu.Unlock()

	_, err := s.rmClient.NodeManagerHeartbeat(context.Background(), &pb.NodeManagerHeartbeatRequest{
		NodeId: s.nodeID,
		Status: &pb.NodeStatus{
			NodeId:       s.nodeID,
			Address:      s.address,
			TotalMemoryMb: s.cfg.TotalMemoryMB,
			UsedMemoryMb:  usedMem,
			TotalCpu:     s.cfg.TotalCPU,
			UsedCpu:      usedCPU,
			NumContainers: int32(len(s.containers)),
			Healthy:      true,
		},
		ContainerStatuses: statuses,
	})
	if err != nil {
		slog.Warn("heartbeat to RM failed", "error", err)
	}
}

// --- gRPC implementations ---

// validateID checks that an ID is safe for use in file paths.
func validateID(id string) error {
	if id == "" {
		return fmt.Errorf("empty ID")
	}
	if strings.ContainsAny(id, `/\`) || strings.Contains(id, "..") {
		return fmt.Errorf("invalid ID: contains path separators or '..': %q", id)
	}
	return nil
}

// safePath resolves a path under a base directory and verifies no traversal.
func safePath(base, child string) (string, error) {
	resolved := filepath.Join(base, child)
	absResolved, err := filepath.Abs(resolved)
	if err != nil {
		return "", err
	}
	absBase, err := filepath.Abs(base)
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(absResolved, absBase+string(filepath.Separator)) && absResolved != absBase {
		return "", fmt.Errorf("path traversal detected: %q escapes %q", child, base)
	}
	return absResolved, nil
}

func (s *Server) LaunchContainer(_ context.Context, req *pb.LaunchContainerRequest) (*pb.LaunchContainerResponse, error) {
	spec := req.Spec
	if spec == nil {
		return &pb.LaunchContainerResponse{Success: false, Error: "nil container spec"}, nil
	}

	// Validate container ID (prevent path traversal)
	if err := validateID(spec.ContainerId); err != nil {
		return &pb.LaunchContainerResponse{Success: false, Error: "invalid container ID: " + err.Error()}, nil
	}

	// Validate command (prevent command injection)
	cleanCmd := filepath.Clean(spec.Command)
	if strings.Contains(cleanCmd, "..") || strings.ContainsAny(cleanCmd, "|;&`$") {
		return &pb.LaunchContainerResponse{Success: false, Error: "invalid command: " + cleanCmd}, nil
	}

	// Hold lock from existence check through insertion (fix TOCTOU race)
	s.mu.Lock()
	if _, exists := s.containers[spec.ContainerId]; exists {
		s.mu.Unlock()
		return &pb.LaunchContainerResponse{Success: false, Error: "container already exists"}, nil
	}

	// Create placeholder to reserve the ID
	cp := &ContainerProcess{
		ContainerID: spec.ContainerId,
		State:       "RUNNING",
		MemoryMB:    spec.MemoryMb,
		CPUVCores:   spec.CpuVcores,
	}
	s.containers[spec.ContainerId] = cp
	s.mu.Unlock()

	// Build the command
	cmd := exec.Command(cleanCmd, spec.Args...)

	// Build minimal isolated environment (don't inherit host env)
	cmd.Env = []string{
		"PATH=/usr/local/bin:/usr/bin:/bin",
		fmt.Sprintf("HOME=/tmp"),
	}
	blockedEnvKeys := map[string]bool{"PATH": true, "LD_PRELOAD": true, "LD_LIBRARY_PATH": true}
	for k, v := range spec.Env {
		if !blockedEnvKeys[k] {
			cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
		}
	}

	// Set process group for clean child process cleanup (platform-specific)
	setSysProcAttr(cmd)

	// Set working directory (with path traversal protection)
	workDir, err := safePath(s.cfg.TempDir, spec.ContainerId)
	if err != nil {
		s.mu.Lock()
		cp.State = "FAILED"
		cp.Diagnostics = err.Error()
		s.mu.Unlock()
		return &pb.LaunchContainerResponse{Success: false, Error: err.Error()}, nil
	}

	if err := os.MkdirAll(workDir, 0755); err != nil {
		s.mu.Lock()
		cp.State = "FAILED"
		cp.Diagnostics = err.Error()
		s.mu.Unlock()
		return &pb.LaunchContainerResponse{Success: false, Error: "create work dir: " + err.Error()}, nil
	}
	cmd.Dir = workDir

	// Redirect output to log files (check errors)
	stdoutFile, err := os.Create(filepath.Join(workDir, "stdout.log"))
	if err != nil {
		s.mu.Lock()
		cp.State = "FAILED"
		cp.Diagnostics = err.Error()
		s.mu.Unlock()
		return &pb.LaunchContainerResponse{Success: false, Error: "create stdout log: " + err.Error()}, nil
	}
	stderrFile, err := os.Create(filepath.Join(workDir, "stderr.log"))
	if err != nil {
		stdoutFile.Close()
		s.mu.Lock()
		cp.State = "FAILED"
		cp.Diagnostics = err.Error()
		s.mu.Unlock()
		return &pb.LaunchContainerResponse{Success: false, Error: "create stderr log: " + err.Error()}, nil
	}
	cmd.Stdout = stdoutFile
	cmd.Stderr = stderrFile

	// Start the process
	if err := cmd.Start(); err != nil {
		stdoutFile.Close()
		stderrFile.Close()
		s.mu.Lock()
		cp.State = "FAILED"
		cp.Diagnostics = err.Error()
		s.mu.Unlock()
		return &pb.LaunchContainerResponse{Success: false, Error: err.Error()}, nil
	}

	s.mu.Lock()
	cp.Cmd = cmd
	s.mu.Unlock()

	// Monitor process in background
	go s.monitorContainer(cp, stdoutFile, stderrFile)

	slog.Info("container launched",
		"containerID", spec.ContainerId,
		"command", cleanCmd,
		"pid", cmd.Process.Pid,
	)
	return &pb.LaunchContainerResponse{Success: true}, nil
}

func (s *Server) monitorContainer(cp *ContainerProcess, stdout, stderr *os.File) {
	defer stdout.Close()
	defer stderr.Close()

	err := cp.Cmd.Wait()

	s.mu.Lock()
	defer s.mu.Unlock()

	if err != nil {
		cp.State = "FAILED"
		if exitErr, ok := err.(*exec.ExitError); ok {
			cp.ExitCode = int32(exitErr.ExitCode())
		}
		cp.Diagnostics = err.Error()
	} else {
		cp.State = "COMPLETED"
		cp.ExitCode = 0
	}

	slog.Info("container finished",
		"containerID", cp.ContainerID,
		"state", cp.State,
		"exitCode", cp.ExitCode,
	)
}

func (s *Server) StopContainer(_ context.Context, req *pb.StopContainerRequest) (*pb.StopContainerResponse, error) {
	s.mu.Lock()
	cp, exists := s.containers[req.ContainerId]
	if !exists {
		s.mu.Unlock()
		return &pb.StopContainerResponse{Success: false, Error: "container not found"}, nil
	}

	if cp.Cmd != nil && cp.Cmd.Process != nil && cp.State == "RUNNING" {
		cp.Cmd.Process.Kill()
		cp.State = "KILLED"
	}
	s.mu.Unlock()

	return &pb.StopContainerResponse{Success: true}, nil
}

func (s *Server) GetContainerStatus(_ context.Context, req *pb.GetContainerStatusRequest) (*pb.GetContainerStatusResponse, error) {
	s.mu.Lock()
	cp, exists := s.containers[req.ContainerId]
	if !exists {
		s.mu.Unlock()
		return &pb.GetContainerStatusResponse{Error: "container not found"}, nil
	}
	// Copy fields under lock
	cpState := cp.State
	cpExitCode := cp.ExitCode
	cpDiag := cp.Diagnostics
	s.mu.Unlock()

	state := pb.ContainerState_CONTAINER_STATE_UNSPECIFIED
	switch cpState {
	case "RUNNING":
		state = pb.ContainerState_CONTAINER_RUNNING
	case "COMPLETED":
		state = pb.ContainerState_CONTAINER_COMPLETED
	case "FAILED":
		state = pb.ContainerState_CONTAINER_FAILED
	case "KILLED":
		state = pb.ContainerState_CONTAINER_KILLED
	}

	return &pb.GetContainerStatusResponse{
		ContainerId: req.ContainerId,
		State:       state,
		ExitCode:    cpExitCode,
		Diagnostics: cpDiag,
	}, nil
}
