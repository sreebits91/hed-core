package benchmark

import (
	"encoding/json"
	"fmt"
	"time"
)

// Result is the stable machine-readable benchmark result.
type Result struct {
	Workers          int           `json:"workers"`
	Requested        int           `json:"requested_transactions"`
	Submitted        uint64        `json:"submitted"`
	Accepted         uint64        `json:"accepted"`
	Committed        uint64        `json:"committed"`
	Failed           uint64        `json:"failed"`
	Rejected         uint64        `json:"rejected"`
	Duration         time.Duration `json:"duration"`
	SubmissionTPS    float64       `json:"submission_tps"`
	CommittedTPS     float64       `json:"committed_tps"`
	MaxQueueDepth    int64         `json:"max_queue_depth"`
	P50              time.Duration `json:"p50_latency"`
	P95              time.Duration `json:"p95_latency"`
	P99              time.Duration `json:"p99_latency"`
	MaxLatency       time.Duration `json:"max_latency"`
}

func (r *Runner) Result(requested int) Result {
	s := r.Metrics.Snapshot()
	return Result{Workers: r.Workers, Requested: requested, Submitted: s.Submitted, Accepted: s.Accepted, Committed: s.Committed, Failed: s.Failed, Rejected: s.Rejected, Duration: s.Elapsed, SubmissionTPS: s.SubmissionTPS, CommittedTPS: s.CommittedTPS, MaxQueueDepth: r.MaxQueueDepth.Load(), P50: s.P50, P95: s.P95, P99: s.P99, MaxLatency: s.MaxLatency}
}

func (r Result) JSON() ([]byte, error) { return json.MarshalIndent(r, "", "  ") }

func (r Result) String() string {
	return fmt.Sprintf("workers=%d requested=%d submitted=%d accepted=%d committed=%d failed=%d rejected=%d duration=%s submission_tps=%.2f committed_tps=%.2f max_queue=%d p50=%s p95=%s p99=%s max=%s", r.Workers, r.Requested, r.Submitted, r.Accepted, r.Committed, r.Failed, r.Rejected, r.Duration, r.SubmissionTPS, r.CommittedTPS, r.MaxQueueDepth, r.P50, r.P95, r.P99, r.MaxLatency)
}
