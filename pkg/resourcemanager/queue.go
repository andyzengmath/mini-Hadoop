package resourcemanager

import (
	"fmt"
	"log/slog"
)

// Queue represents a resource queue in the capacity scheduler.
type Queue struct {
	Name            string
	MinCapacityPct  float64 // Guaranteed minimum capacity (0.0-1.0)
	MaxCapacityPct  float64 // Maximum capacity (allows bursting, 0.0-1.0)
	UsedMemoryMB    int32
	UsedCPU         int32
	PendingApps     []string // App IDs waiting in this queue
}

// QueueConfig defines a queue's capacity configuration.
type QueueConfig struct {
	Name           string  `json:"name"`
	MinCapacityPct float64 `json:"min_capacity_pct"` // e.g., 0.5 = 50%
	MaxCapacityPct float64 `json:"max_capacity_pct"` // e.g., 1.0 = can burst to 100%
}

// QueueManager manages multiple resource queues for the capacity scheduler.
type QueueManager struct {
	queues       map[string]*Queue
	defaultQueue string
}

// NewQueueManager creates a queue manager with default configuration.
// If no queues are configured, creates a single "default" queue with 100% capacity.
func NewQueueManager(configs []QueueConfig) *QueueManager {
	qm := &QueueManager{
		queues:       make(map[string]*Queue),
		defaultQueue: "default",
	}

	if len(configs) == 0 {
		// Single default queue — behaves like FIFO
		qm.queues["default"] = &Queue{
			Name:           "default",
			MinCapacityPct: 1.0,
			MaxCapacityPct: 1.0,
		}
	} else {
		for _, cfg := range configs {
			qm.queues[cfg.Name] = &Queue{
				Name:           cfg.Name,
				MinCapacityPct: cfg.MinCapacityPct,
				MaxCapacityPct: cfg.MaxCapacityPct,
			}
			if qm.defaultQueue == "default" {
				qm.defaultQueue = cfg.Name // first configured queue is default
			}
		}
	}

	slog.Info("queue manager initialized", "queues", len(qm.queues))
	return qm
}

// GetQueue returns a queue by name or the default queue.
func (qm *QueueManager) GetQueue(name string) (*Queue, error) {
	if name == "" {
		name = qm.defaultQueue
	}
	q, exists := qm.queues[name]
	if !exists {
		return nil, fmt.Errorf("queue %q not found", name)
	}
	return q, nil
}

// CanAllocate checks if a queue can accept more resources given the total cluster capacity.
func (qm *QueueManager) CanAllocate(queueName string, memMB, cpu int32, totalClusterMemMB, totalClusterCPU int32) bool {
	q, err := qm.GetQueue(queueName)
	if err != nil {
		return false
	}

	maxMem := int32(float64(totalClusterMemMB) * q.MaxCapacityPct)
	if q.UsedMemoryMB+memMB > maxMem {
		return false
	}

	return true
}

// Allocate records resource usage for a queue.
func (qm *QueueManager) Allocate(queueName string, memMB, cpu int32) {
	q, err := qm.GetQueue(queueName)
	if err != nil {
		return
	}
	q.UsedMemoryMB += memMB
	q.UsedCPU += cpu
}

// Release returns resources to a queue.
func (qm *QueueManager) Release(queueName string, memMB, cpu int32) {
	q, err := qm.GetQueue(queueName)
	if err != nil {
		return
	}
	q.UsedMemoryMB -= memMB
	q.UsedCPU -= cpu
	if q.UsedMemoryMB < 0 {
		q.UsedMemoryMB = 0
	}
	if q.UsedCPU < 0 {
		q.UsedCPU = 0
	}
}

// ListQueues returns all queue names and their usage.
func (qm *QueueManager) ListQueues() []map[string]interface{} {
	var result []map[string]interface{}
	for _, q := range qm.queues {
		result = append(result, map[string]interface{}{
			"name":             q.Name,
			"min_capacity_pct": q.MinCapacityPct,
			"max_capacity_pct": q.MaxCapacityPct,
			"used_memory_mb":   q.UsedMemoryMB,
			"used_cpu":         q.UsedCPU,
		})
	}
	return result
}
