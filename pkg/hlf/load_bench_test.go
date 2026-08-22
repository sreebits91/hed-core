package hlf

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"hed-core/pkg/engine"
)

// These benchmarks exercise the HED committer hot path only. They deliberately
// do not claim Fabric end-to-end TPS: flushBatch is the handoff/accounting
// boundary, while real Fabric ordering, endorsement and validation must be
// benchmarked separately.
func BenchmarkHLFLoad100K(b *testing.B) { benchmarkHLFLoad(b, 100_000) }
func BenchmarkHLFLoad250K(b *testing.B) { benchmarkHLFLoad(b, 250_000) }
func BenchmarkHLFLoad500K(b *testing.B) { benchmarkHLFLoad(b, 500_000) }

func percentileMicros(samples []int64, p float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	idx := int(float64(len(samples)-1) * p)
	return float64(samples[idx])
}

func benchmarkHLFLoad(b *testing.B, target int) {
	b.Helper()

	const workers = 64
	const latencySampleEvery = 100
	cfg := BatchConfig{
		MaxBatchSize: 2000,
		FlushTimeout: 2 * time.Millisecond,
		WorkerCount:  workers,
		QueueSize:    target + 100_000,
	}

	b.ReportAllocs()
	b.SetBytes(1)
	b.ResetTimer()

	for iteration := 0; iteration < b.N; iteration++ {
		c := NewHLFCommitter(cfg)
		start := time.Now()

		var next atomic.Int64
		var rejected atomic.Uint64
		latencies := make([]int64, 0, target/latencySampleEvery+workers)
		var latencyMu sync.Mutex
		var wg sync.WaitGroup
		wg.Add(workers)

		for w := 0; w < workers; w++ {
			go func(workerID int) {
				defer wg.Done()
				for {
					i := int(next.Add(1)) - 1
					if i >= target {
						return
					}
					tx := &engine.TxPayload{
						TxUUID:    fmt.Sprintf("bench-%d-%d", workerID, i),
						AccountID: "load-test",
						Amount:    int64(i),
					}
					sample := i%latencySampleEvery == 0
					var submitStart time.Time
					if sample {
						submitStart = time.Now()
					}
					if !c.SubmitTx(tx) {
						rejected.Add(1)
					}
					if sample {
						latencyMu.Lock()
						latencies = append(latencies, time.Since(submitStart).Microseconds())
						latencyMu.Unlock()
					}
				}
			}(w)
		}
		wg.Wait()

		submitElapsed := time.Since(start)
		for c.TotalCommitted()+c.TotalFailed() < uint64(target) && time.Since(start) < 30*time.Second {
			time.Sleep(time.Millisecond)
		}
		totalElapsed := time.Since(start)

		committed := c.TotalCommitted()
		dropped := c.TotalDropped()
		failed := c.TotalFailed()
		c.Stop()

		if committed+dropped != uint64(target) {
			b.Fatalf("load=%d: accounting mismatch committed=%d dropped=%d target=%d", target, committed, dropped, target)
		}

		b.ReportMetric(float64(committed)/totalElapsed.Seconds(), "committed-tx/s")
		b.ReportMetric(float64(target)/submitElapsed.Seconds(), "offered-tx/s")
		b.ReportMetric(float64(dropped)/float64(target)*100, "drop-%")
		b.ReportMetric(float64(failed)/float64(target)*100, "failure-%")
		b.ReportMetric(percentileMicros(latencies, 0.50), "submit-p50-us")
		b.ReportMetric(percentileMicros(latencies, 0.95), "submit-p95-us")
		b.ReportMetric(percentileMicros(latencies, 0.99), "submit-p99-us")
		b.ReportMetric(float64(c.QueueCapacity()), "queue-capacity")

		if rejected.Load() != dropped {
			b.Fatalf("load=%d: rejected=%d dropped=%d", target, rejected.Load(), dropped)
		}
		if dropped != 0 {
			b.Logf("load=%d saturated: dropped=%d (%.3f%%)", target, dropped, float64(dropped)/float64(target)*100)
		}
	}
}

// TestHLFLoadLevels is useful for CI because it executes each load level once
// instead of relying on the benchmark harness's repeated b.N iterations.
func TestHLFLoadLevels(t *testing.T) {
	for _, target := range []int{100_000, 250_000, 500_000} {
		t.Run(fmt.Sprintf("%d", target), func(t *testing.T) {
			c := NewHLFCommitter(BatchConfig{
				MaxBatchSize: 2000,
				FlushTimeout: 2 * time.Millisecond,
				WorkerCount:  64,
				QueueSize:    target + 100_000,
			})
			start := time.Now()
			for i := 0; i < target; i++ {
				if !c.SubmitTx(&engine.TxPayload{TxUUID: fmt.Sprintf("test-%d", i), AccountID: "load-test", Amount: int64(i)}) {
					t.Fatalf("transaction %d rejected", i)
				}
			}
			for c.TotalCommitted() < uint64(target) && time.Since(start) < 30*time.Second {
				time.Sleep(time.Millisecond)
			}
			elapsed := time.Since(start)
			committed := c.TotalCommitted()
			dropped := c.TotalDropped()
			c.Stop()

			if committed != uint64(target) || dropped != 0 {
				t.Fatalf("committed=%d dropped=%d target=%d", committed, dropped, target)
			}
			t.Logf("target=%d committed=%d elapsed=%s throughput=%.0f tx/s", target, committed, elapsed, float64(committed)/elapsed.Seconds())
		})
	}
}
