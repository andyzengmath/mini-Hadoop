package mapreduce

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/mini-hadoop/mini-hadoop/pkg/rpc"

	pb "github.com/mini-hadoop/mini-hadoop/proto"

	"google.golang.org/grpc"
)

// TaskState represents the lifecycle state of a map or reduce task.
type TaskState string

const (
	TaskPending   TaskState = "PENDING"
	TaskRunning   TaskState = "RUNNING"
	TaskCompleted TaskState = "COMPLETED"
	TaskFailed    TaskState = "FAILED"
)

// TaskInfo tracks a single map or reduce task.
type TaskInfo struct {
	TaskID      string
	TaskType    string // "MAP" or "REDUCE"
	State       TaskState
	ContainerID string
	NodeID      string
	NodeAddr    string
	Attempts    int
	MaxAttempts int
	Split       *InputSplit  // For map tasks
	PartitionID int          // For reduce tasks
	OutputPaths []string     // Partition output files (map tasks)
	IsDataLocal bool
}

// MRAppMaster is the per-job coordinator that manages map and reduce tasks.
// It requests containers from the ResourceManager with locality hints,
// launches tasks via NodeManagers, tracks completion, and handles retries.
type MRAppMaster struct {
	mu sync.Mutex

	jobID       string
	cfg         JobConfig
	appID       string

	rmConn      *grpc.ClientConn
	rmClient    pb.ResourceManagerServiceClient
	namenodeAddr string

	splits      []InputSplit
	mapTasks    []*TaskInfo
	reduceTasks []*TaskInfo

	// map task ID -> node address where output is available
	mapOutputLocations map[string]string

	totalMapTasks    int
	completedMaps    int
	totalReduceTasks int
	completedReduces int

	dataLocalMaps int
	state         string // INITIALIZING, MAP_PHASE, REDUCE_PHASE, COMPLETED, FAILED
	diagnostics   string
}

// NewMRAppMaster creates a new ApplicationMaster for a MapReduce job.
func NewMRAppMaster(jobID, appID string, cfg JobConfig, rmAddr, namenodeAddr string) (*MRAppMaster, error) {
	conn, err := rpc.DialWithRetry(rmAddr, 3)
	if err != nil {
		return nil, fmt.Errorf("connect to ResourceManager: %w", err)
	}

	return &MRAppMaster{
		jobID:              jobID,
		cfg:                cfg,
		appID:              appID,
		rmConn:             conn,
		rmClient:           pb.NewResourceManagerServiceClient(conn),
		namenodeAddr:       namenodeAddr,
		mapOutputLocations: make(map[string]string),
		state:              "INITIALIZING",
	}, nil
}

// Run executes the full MapReduce job lifecycle.
func (am *MRAppMaster) Run() error {
	defer am.rmConn.Close()

	slog.Info("MRAppMaster starting", "job_id", am.jobID, "app_id", am.appID)

	// Phase 1: Compute input splits
	splits, err := ComputeSplits(am.namenodeAddr, am.cfg.InputPath)
	if err != nil {
		am.fail("compute splits: " + err.Error())
		return err
	}
	am.splits = splits
	am.totalMapTasks = len(splits)
	am.totalReduceTasks = am.cfg.NumReducers

	slog.Info("input splits computed",
		"job_id", am.jobID,
		"num_splits", len(splits),
		"num_reducers", am.cfg.NumReducers,
	)

	// Phase 2: Run map tasks
	am.state = "MAP_PHASE"
	if err := am.runMapPhase(); err != nil {
		am.fail("map phase: " + err.Error())
		return err
	}

	// Phase 3: Run reduce tasks
	am.state = "REDUCE_PHASE"
	if err := am.runReducePhase(); err != nil {
		am.fail("reduce phase: " + err.Error())
		return err
	}

	// Phase 4: Complete
	am.state = "COMPLETED"
	am.emitLocalityStats()

	slog.Info("job completed successfully", "job_id", am.jobID)
	return nil
}

// runMapPhase requests containers with locality hints and runs all map tasks.
func (am *MRAppMaster) runMapPhase() error {
	// Initialize map tasks
	am.mapTasks = make([]*TaskInfo, len(am.splits))
	for i, split := range am.splits {
		splitCopy := split
		am.mapTasks[i] = &TaskInfo{
			TaskID:      fmt.Sprintf("map-%03d", i),
			TaskType:    "MAP",
			State:       TaskPending,
			MaxAttempts: am.cfg.MaxTaskRetries,
			Split:       &splitCopy,
		}
	}

	// Request containers with locality hints and run tasks
	for _, task := range am.mapTasks {
		if err := am.runTaskWithRetry(task); err != nil {
			return fmt.Errorf("map task %s: %w", task.TaskID, err)
		}
	}

	slog.Info("map phase complete",
		"job_id", am.jobID,
		"total_maps", am.totalMapTasks,
		"completed", am.completedMaps,
		"data_local", am.dataLocalMaps,
	)
	return nil
}

// runReducePhase requests containers and runs all reduce tasks.
func (am *MRAppMaster) runReducePhase() error {
	am.reduceTasks = make([]*TaskInfo, am.totalReduceTasks)
	for i := 0; i < am.totalReduceTasks; i++ {
		am.reduceTasks[i] = &TaskInfo{
			TaskID:      fmt.Sprintf("reduce-%03d", i),
			TaskType:    "REDUCE",
			State:       TaskPending,
			MaxAttempts: am.cfg.MaxTaskRetries,
			PartitionID: i,
		}
	}

	for _, task := range am.reduceTasks {
		if err := am.runTaskWithRetry(task); err != nil {
			return fmt.Errorf("reduce task %s: %w", task.TaskID, err)
		}
	}

	slog.Info("reduce phase complete",
		"job_id", am.jobID,
		"total_reduces", am.totalReduceTasks,
		"completed", am.completedReduces,
	)
	return nil
}

// runTaskWithRetry runs a task, retrying on failure. MaxAttempts is the total
// number of attempts (not retries). E.g., MaxAttempts=3 means up to 3 tries.
func (am *MRAppMaster) runTaskWithRetry(task *TaskInfo) error {
	for attempt := 0; attempt < task.MaxAttempts; attempt++ {
		task.Attempts = attempt + 1
		task.State = TaskPending

		// Request container
		container, err := am.requestContainer(task)
		if err != nil {
			slog.Warn("container allocation failed, retrying",
				"task_id", task.TaskID,
				"attempt", attempt+1,
				"error", err,
			)
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}

		task.ContainerID = container.ContainerId
		task.NodeID = container.NodeId
		task.NodeAddr = container.Env["NODE_ADDRESS"]

		// Check data locality for map tasks
		if task.TaskType == "MAP" && task.Split != nil {
			task.IsDataLocal = isDataLocal(task.NodeAddr, task.Split.Locations)
		}

		// Emit structured task launch log
		am.emitTaskLaunchLog(task)

		// Launch task
		task.State = TaskRunning
		err = am.launchTask(task, container)
		if err != nil {
			task.State = TaskFailed
			slog.Warn("task failed, retrying",
				"task_id", task.TaskID,
				"attempt", attempt+1,
				"error", err,
			)
			// Release failed container
			am.releaseContainer(container.ContainerId)
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}

		// Wait for completion
		if err := am.waitForTask(task, container); err != nil {
			task.State = TaskFailed
			slog.Warn("task execution failed",
				"task_id", task.TaskID,
				"attempt", attempt+1,
				"error", err,
			)
			am.releaseContainer(container.ContainerId)
			continue
		}

		// Success
		task.State = TaskCompleted
		am.mu.Lock()
		if task.TaskType == "MAP" {
			am.completedMaps++
			am.mapOutputLocations[task.TaskID] = task.NodeAddr
			if task.IsDataLocal {
				am.dataLocalMaps++
			}
		} else {
			am.completedReduces++
		}
		am.mu.Unlock()

		am.releaseContainer(container.ContainerId)
		return nil
	}

	return fmt.Errorf("task %s failed after %d attempts", task.TaskID, task.MaxAttempts)
}

// requestContainer asks the RM for a container, with locality hints for map tasks.
func (am *MRAppMaster) requestContainer(task *TaskInfo) (*pb.ContainerSpec, error) {
	req := &pb.AllocateContainersRequest{
		AppId: am.appID,
		Requests: []*pb.ContainerRequest{{
			MemoryMb:  512,
			CpuVcores: 1,
			Count:     1,
			RequestId: task.TaskID,
		}},
	}

	// Add locality hints for map tasks
	if task.TaskType == "MAP" && task.Split != nil && len(task.Split.Locations) > 0 {
		req.Requests[0].Locality = &pb.LocalityPreference{
			PreferredNodes: task.Split.Locations,
			Relaxed:        true, // Allow non-local if preferred unavailable
		}
	}

	resp, err := am.rmClient.AllocateContainers(context.Background(), req)
	if err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("%s", resp.Error)
	}
	if len(resp.Containers) == 0 {
		return nil, fmt.Errorf("no containers allocated")
	}

	return resp.Containers[0], nil
}

// launchTask sends the task to the NodeManager for execution.
func (am *MRAppMaster) launchTask(task *TaskInfo, container *pb.ContainerSpec) error {
	if task.NodeAddr == "" {
		return fmt.Errorf("no node address for container %s", container.ContainerId)
	}

	conn, err := rpc.Dial(task.NodeAddr)
	if err != nil {
		return fmt.Errorf("connect to NodeManager: %w", err)
	}
	defer conn.Close()

	nmClient := pb.NewNodeManagerServiceClient(conn)

	// Build the container spec with task-specific command and args
	var command string
	var args []string

	if task.TaskType == "MAP" {
		command = "mapworker"
		args = []string{
			"--mode", "map",
			"--task-id", task.TaskID,
			"--job-id", am.jobID,
			"--mapper", am.cfg.MapperName,
			"--block-id", task.Split.BlockID,
			"--namenode", am.namenodeAddr,
			"--num-reducers", fmt.Sprintf("%d", am.cfg.NumReducers),
		}
	} else {
		command = "mapworker"
		args = []string{
			"--mode", "reduce",
			"--task-id", task.TaskID,
			"--job-id", am.jobID,
			"--reducer", am.cfg.ReducerName,
			"--partition-id", fmt.Sprintf("%d", task.PartitionID),
			"--namenode", am.namenodeAddr,
			"--output", am.cfg.OutputPath,
		}
	}

	launchSpec := &pb.ContainerSpec{
		ContainerId: container.ContainerId,
		NodeId:      container.NodeId,
		MemoryMb:    container.MemoryMb,
		CpuVcores:   container.CpuVcores,
		Command:     command,
		Args:        args,
		Env:         container.Env,
	}

	resp, err := nmClient.LaunchContainer(context.Background(), &pb.LaunchContainerRequest{
		Spec: launchSpec,
	})
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("launch failed: %s", resp.Error)
	}

	return nil
}

// waitForTask polls the NodeManager until the task container exits.
func (am *MRAppMaster) waitForTask(task *TaskInfo, container *pb.ContainerSpec) error {
	if task.NodeAddr == "" {
		return fmt.Errorf("no node address")
	}

	conn, err := rpc.Dial(task.NodeAddr)
	if err != nil {
		return fmt.Errorf("connect to NM for status: %w", err)
	}
	defer conn.Close()

	nmClient := pb.NewNodeManagerServiceClient(conn)

	for i := 0; i < 600; i++ { // Max 10 minutes
		time.Sleep(1 * time.Second)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := nmClient.GetContainerStatus(ctx, &pb.GetContainerStatusRequest{
			ContainerId: container.ContainerId,
		})
		cancel()
		if err != nil {
			// NM might be down — treat as task failure
			return fmt.Errorf("get status: %w", err)
		}

		switch resp.State {
		case pb.ContainerState_CONTAINER_COMPLETED:
			if resp.ExitCode != 0 {
				return fmt.Errorf("task exited with code %d: %s", resp.ExitCode, resp.Diagnostics)
			}
			return nil
		case pb.ContainerState_CONTAINER_FAILED:
			return fmt.Errorf("container failed: %s", resp.Diagnostics)
		case pb.ContainerState_CONTAINER_KILLED:
			return fmt.Errorf("container killed: %s", resp.Diagnostics)
		case pb.ContainerState_CONTAINER_RUNNING:
			continue
		}
	}

	return fmt.Errorf("task timed out after 10 minutes")
}

// releaseContainer tells the RM to free a container's resources.
func (am *MRAppMaster) releaseContainer(containerID string) {
	am.rmClient.ReleaseContainers(context.Background(), &pb.ReleaseContainersRequest{
		AppId:        am.appID,
		ContainerIds: []string{containerID},
	})
}

// emitTaskLaunchLog emits a structured JSON log line per the plan (Critic MEDIUM-1).
func (am *MRAppMaster) emitTaskLaunchLog(task *TaskInfo) {
	requestedNode := ""
	if task.Split != nil && len(task.Split.Locations) > 0 {
		requestedNode = task.Split.Locations[0]
	}

	slog.Info("task_launched",
		"task_id", task.TaskID,
		"task_type", task.TaskType,
		"requested_node", requestedNode,
		"allocated_node", task.NodeAddr,
		"is_data_local", task.IsDataLocal,
	)
}

// emitLocalityStats emits a job-completion locality summary.
func (am *MRAppMaster) emitLocalityStats() {
	percentage := float64(0)
	if am.totalMapTasks > 0 {
		percentage = float64(am.dataLocalMaps) / float64(am.totalMapTasks) * 100
	}

	slog.Info("locality_stats",
		"job_id", am.jobID,
		"total_map_tasks", am.totalMapTasks,
		"data_local", am.dataLocalMaps,
		"data_local_percentage", fmt.Sprintf("%.1f", percentage),
	)
}

func (am *MRAppMaster) fail(msg string) {
	am.state = "FAILED"
	am.diagnostics = msg
	slog.Error("job failed", "job_id", am.jobID, "error", msg)
}

// isDataLocal checks if the allocated node is one of the block's locations.
func isDataLocal(nodeAddr string, blockLocations []string) bool {
	for _, loc := range blockLocations {
		if loc == nodeAddr {
			return true
		}
	}
	return false
}
