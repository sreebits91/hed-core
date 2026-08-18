package benchmark

import (
	"testing"
	"time"
)

func TestMetricsSeparatesSubmissionAndCommitTPS(t *testing.T) {
	m := NewMetrics()
	m.StartedAt = time.Now().Add(-2 * time.Second)
	for i := 0; i < 10; i++ { m.RecordSubmission(); m.RecordAccepted() }
	for i := 0; i < 7; i++ { m.RecordCommitted(time.Duration(i+1) * time.Millisecond) }
	for i := 0; i < 3; i++ { m.RecordFailed() }
	m.Finish()

	s := m.Snapshot()
	if s.Submitted != 10 || s.Accepted != 10 || s.Committed != 7 || s.Failed != 3 {
		t.Fatalf("unexpected counts: %+v", s)
	}
	if s.SubmissionTPS <= s.CommittedTPS {
		t.Fatalf("submission TPS %.2f should exceed committed TPS %.2f", s.SubmissionTPS, s.CommittedTPS)
	}
	if s.P50 == 0 || s.P95 == 0 || s.P99 == 0 || s.MaxLatency != 7*time.Millisecond {
		t.Fatalf("unexpected latency metrics: p50=%s p95=%s p99=%s max=%s", s.P50, s.P95, s.P99, s.MaxLatency)
	}
}

func TestMetricsConcurrentRecording(t *testing.T) {
	m := NewMetrics()
	done := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func() {
			for j := 0; j < 1000; j++ {
				m.RecordSubmission()
				m.RecordAccepted()
				m.RecordCommitted(time.Microsecond)
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < 8; i++ { <-done }
	m.Finish()
	s := m.Snapshot()
	if s.Submitted != 8000 || s.Accepted != 8000 || s.Committed != 8000 {
		t.Fatalf("unexpected concurrent counts: %+v", s)
	}
}
