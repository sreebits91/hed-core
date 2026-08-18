package benchmark

import (
	"encoding/json"
	"fmt"
	"time"
)

// BenchmarkReport is the machine-readable extension of the legacy benchmark
// Result. It deliberately does not redefine Result so existing callers remain
// source-compatible.
type BenchmarkReport struct {
	Workers       int           `json:"workers"`
	Requested     int           `json:"requested_transactions"`
	Completed     uint64        `json:"completed_transactions"`
	Duration      time.Duration `json:"duration"`
	TPS           float64       `json:"tps"`
	MaxQueueDepth int64         `json:"max_queue_depth"`
}

func NewBenchmarkReport(r Result, requested int) BenchmarkReport {
	return BenchmarkReport{
		Workers: r.WorkerCount,
		Requested: requested,
		Completed: r.TotalTx,
		Duration: r.Duration,
		TPS: r.TPS,
		MaxQueueDepth: r.MaxQueueDepth,
	}
}

func (r BenchmarkReport) JSON() ([]byte, error) {
	return json.MarshalIndent(r, "", "  ")
}

func (r BenchmarkReport) String() string {
	return fmt.Sprintf("workers=%d requested=%d completed=%d duration=%s tps=%.2f max_queue=%d", r.Workers, r.Requested, r.Completed, r.Duration, r.TPS, r.MaxQueueDepth)
}
