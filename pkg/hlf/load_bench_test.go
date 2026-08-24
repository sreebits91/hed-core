package hlf

import (
	"fmt"
	"sort"
	"sync"
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
func BenchmarkHLFLoad750K(b *testing.B) { benchmarkHLFLoad(b, 750_000) }
func BenchmarkHLFLoad1M(b *testing.B)   { benchmarkHLFLoad(b, 1_000_000) }
func BenchmarkHLFLoad2M(b *testing.B)   { benchmarkHLFLoad(b, 2_000_000) }
func BenchmarkHLFLoad5M(b *testing.B)   { benchmarkHLFLoad(b, 5_000_000) }

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

	// Pre-build transactions outside the timed region. The previous benchmark
	// generated a UUID string and allocated a TxPayload for every transaction,
	// making the benchmark measure its own test-data generator instead of the
	// committer hot path. The committer only needs the payload pointer here.
	txs := make([]engine.TxPayload, target)
	for i := range txs {
		txs[i] = engine.TxPayload{
			TxUUID:    "bench",
			AccountID: "load-test",
			Amount:    int64(i),
		}
	}

	b.ReportAllocs()
	b.SetBytes(1)
	b.ResetTimer()

	for iteration := 0; iteration < b.N; iteration++ {
		c := NewHLFCommitter(cfg)
		start := time.Now()

		var rejected uint64
		var wg sync.WaitGroup
		wg.Add(workers)
		latencies := make([]int64, (target-1)/latencySampleEvery+1)

		// Give each producer a fixed strided range. This removes the atomic
		// fetch-add contention from the benchmark producer itself.
		for w := 0; w < workers; w++ {
			go func(workerID int) {
				defer wg.Done()
				for i := workerID; i < target; i += workers {
					sample := i%latencySampleEvery == 0
					var submitStart time.Time
					if sample {
						submitStart = time.Now()
					}
					if !c.SubmitTx(&txs[i]) {
						rejected++
					}
					if sample {
						latencies[i/latencySampleEvery] = time.Since(submitStart).Microseconds()
					}
				}
			}(w)
		}
		wg.Wait()

		submitElapsed := time.Since(start)
		for c.TotalCommitted()+c.TotalFailed() < uint64(target) && time.Since(start) < 60*time.Second {
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

		if rejected != dropped {
			b.Fatalf("load=%d: rejected=%d dropped=%d", target, rejected, dropped)
		}
		if dropped != 0 {
			b.Logf("load=%d saturated: dropped=%d (%.3f%%)", target, dropped, float64(dropped)/float64(target)*100)
		}
	}
}

// TestHLFLoadLevels executes each load level once for CI. The benchmark
// variants above are used for detailed throughput and latency measurements.
func TestHLFLoadLevels(t *testing.T) {
	for _, target := range []int{100_000, 250_000, 500_000, 750_000, 1_000_000, 2_000_000, 5_000_000} {
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
			for c.TotalCommitted() < uint64(target) && time.Since(start) < 60*time.Second {
				time.Sleep(time.Millisecond)
			}
			elapsed := time.Since(start)
			committed := c.TotalCommitted()
			dropped := c.TotalDropped()
			failed := c.TotalFailed()
			c.Stop()

			if committed != uint64(target) || dropped != 0 || failed != 0 {
				t.Fatalf("committed=%d dropped=%d failed=%d target=%d", committed, dropped, failed, target)
			}
			t.Logf("target=%d committed=%d elapsed=%s throughput=%.0f tx/s", target, committed, elapsed, float64(committed)/elapsed.Seconds())
		})
	}
}
