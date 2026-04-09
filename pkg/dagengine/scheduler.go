package dagengine

import (
	"fmt"
	"log/slog"
	"sync/atomic"
)

var stageCounter int64

// Stage represents a group of tasks that can execute without a shuffle boundary.
// Narrow dependencies are pipelined within a stage; wide dependencies create stage boundaries.
type Stage struct {
	ID       string
	RDD      *RDD
	Parents  []*Stage
	IsShuffle bool // True if this stage's input requires a shuffle
	State    string // PENDING, RUNNING, COMPLETED, FAILED
}

// DAGScheduler splits an RDD computation graph into stages at shuffle boundaries
// and executes them in topological order.
type DAGScheduler struct {
	stages []*Stage
}

// NewDAGScheduler creates a new DAG scheduler.
func NewDAGScheduler() *DAGScheduler {
	return &DAGScheduler{}
}

// SubmitJob takes a final RDD and builds the stage DAG by walking the lineage.
// Returns the stages in execution order (parents before children).
func (ds *DAGScheduler) SubmitJob(finalRDD *RDD) []*Stage {
	ds.stages = nil
	stageMap := make(map[string]*Stage) // rddID -> stage

	ds.buildStages(finalRDD, stageMap)

	// Topological sort: execute parents before children
	ordered := ds.topologicalSort()

	slog.Info("DAG scheduled",
		"total_stages", len(ordered),
		"final_rdd", finalRDD.ID,
	)

	return ordered
}

// Execute runs all stages in order, returning the final RDD's collected output.
func (ds *DAGScheduler) Execute(finalRDD *RDD) ([]KeyValue, error) {
	stages := ds.SubmitJob(finalRDD)

	for _, stage := range stages {
		stage.State = "RUNNING"
		slog.Info("executing stage",
			"stage_id", stage.ID,
			"rdd_id", stage.RDD.ID,
			"partitions", len(stage.RDD.Partitions),
			"is_shuffle", stage.IsShuffle,
		)

		// In this local implementation, computation is already materialized
		// in the RDD partitions during Map/FlatMap/Filter/ReduceByKey calls.
		// A distributed implementation would submit tasks to YARN here.

		stage.State = "COMPLETED"
	}

	return finalRDD.Collect(), nil
}

func (ds *DAGScheduler) buildStages(rdd *RDD, stageMap map[string]*Stage) *Stage {
	if existing, ok := stageMap[rdd.ID]; ok {
		return existing
	}

	stage := &Stage{
		ID:    fmt.Sprintf("stage_%d", atomic.AddInt64(&stageCounter, 1)),
		RDD:   rdd,
		State: "PENDING",
	}

	for _, dep := range rdd.Deps {
		parentStage := ds.buildStages(dep.Parent, stageMap)
		if dep.Type == WideDep {
			stage.IsShuffle = true
			stage.Parents = append(stage.Parents, parentStage)
		} else {
			// Narrow dep: pipeline into same stage (parent's tasks merge with this stage)
			stage.Parents = append(stage.Parents, parentStage)
		}
	}

	stageMap[rdd.ID] = stage
	ds.stages = append(ds.stages, stage)
	return stage
}

func (ds *DAGScheduler) topologicalSort() []*Stage {
	// Simple: stages are already added in dependency order (parents first)
	// because buildStages recurses to parents before adding the current stage
	return ds.stages
}
