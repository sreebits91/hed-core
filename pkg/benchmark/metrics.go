package benchmark

import (
	"sort"
	"sync"
	"time"
)

// Metrics contains client-side submission and authoritative commit measurements.
type Metrics struct {
	mu sync.Mutex

	StartedAt time.Time
	EndedAt   time.Time

	Submitted uint64
	Accepted  uint64
	Committed uint64
	Failed    uint64
	Rejected  uint64

	latencies []time.Duration
}

func NewMetrics() *Metrics {
	return &Metrics{StartedAt: time.Now(), latencies: make([]time.Duration, 0)}
}

func (m *Metrics) RecordSubmission() { m.mu.Lock(); m.Submitted++; m.mu.Unlock() }
func (m *Metrics) RecordAccepted()  { m.mu.Lock(); m.Accepted++; m.mu.Unlock() }
func (m *Metrics) RecordRejected()  { m.mu.Lock(); m.Rejected++; m.mu.Unlock() }
func (m *Metrics) RecordCommitted(latency time.Duration) {
	m.mu.Lock()
	m.Committed++
	if latency >= 0 {
		m.latencies = append(m.latencies, latency)
	}
	m.mu.Unlock()
}
func (m *Metrics) RecordFailed() { m.mu.Lock(); m.Failed++; m.mu.Unlock() }
func (m *Metrics) Finish() { m.mu.Lock(); m.EndedAt = time.Now(); m.mu.Unlock() }

func (m *Metrics) Snapshot() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	latencies := append([]time.Duration(nil), m.latencies...)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	elapsed := m.EndedAt.Sub(m.StartedAt)
	if m.EndedAt.IsZero() { elapsed = time.Since(m.StartedAt) }
	return Snapshot{
		StartedAt: m.StartedAt, EndedAt: m.EndedAt, Elapsed: elapsed,
		Submitted: m.Submitted, Accepted: m.Accepted, Committed: m.Committed,
		Failed: m.Failed, Rejected: m.Rejected,
		SubmissionTPS: rate(m.Accepted, elapsed), CommittedTPS: rate(m.Committed, elapsed),
		P50: percentile(latencies, .50), P95: percentile(latencies, .95), P99: percentile(latencies, .99),
		MaxLatency: maxLatency(latencies),
	}
}

type Snapshot struct {
	StartedAt, EndedAt time.Time
	Elapsed time.Duration
	Submitted, Accepted, Committed, Failed, Rejected uint64
	SubmissionTPS, CommittedTPS float64
	P50, P95, P99, MaxLatency time.Duration
}

func rate(count uint64, elapsed time.Duration) float64 {
	if elapsed <= 0 { return 0 }
	return float64(count) / elapsed.Seconds()
}

func percentile(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 { return 0 }
	idx := int(float64(len(values)-1) * p)
	return values[idx]
}

func maxLatency(values []time.Duration) time.Duration {
	if len(values) == 0 { return 0 }
	return values[len(values)-1]
}
