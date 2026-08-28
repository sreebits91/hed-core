package hlf

import (
	"runtime"
	"testing"
	"time"

	"hed-core/pkg/engine"
)

func newBenchmarkCommitter() *HLFCommitter {
	return NewHLFCommitter(BatchConfig{
		MaxBatchSize: 2000,
		FlushTimeout: time.Millisecond,
		WorkerCount:  32,
		QueueSize:    500000,
	})
}

func submitLoad(t testing.TB, c *HLFCommitter, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		tx := &engine.TxPayload{TxUUID: engine.GenerateUUID(), AccountID: "bench", Amount: int64(i)}
		for !c.SubmitTx(tx) {
			runtime.Gosched()
		}
	}

	deadline := time.Now().Add(30 * time.Second)
	for c.TotalCommitted() < uint64(n) {
		if time.Now().After(deadline) {
			t.Fatalf("commit timeout: committed=%d want=%d dropped=%d failed=%d", c.TotalCommitted(), n, c.TotalDropped(), c.TotalFailed())
		}
		runtime.Gosched()
	}
}

func TestHLFLoadLevels(t *testing.T) {
	levels := []int{100_000, 250_000, 500_000}
	for _, n := range levels {
		t.Run(loadLevelName(n), func(t *testing.T) {
			c := newBenchmarkCommitter()
			defer c.Stop()
			submitLoad(t, c, n)
			if got := c.TotalDropped(); got != 0 {
				t.Fatalf("dropped=%d want=0", got)
			}
		})
	}
}

func loadLevelName(n int) string {
	switch n {
	case 100_000:
		return "100K"
	case 250_000:
		return "250K"
	case 500_000:
		return "500K"
	default:
		return "load"
	}
}

func benchmarkHLFLoad(b *testing.B, level int) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()

	for iteration := 0; iteration < b.N; iteration++ {
		c := newBenchmarkCommitter()
		for i := 0; i < level; i++ {
			tx := &engine.TxPayload{TxUUID: engine.GenerateUUID(), AccountID: "bench", Amount: int64(i)}
			for !c.SubmitTx(tx) {
				runtime.Gosched()
			}
		}
		b.StopTimer()
		deadline := time.Now().Add(30 * time.Second)
		for c.TotalCommitted() < uint64(level) {
			if time.Now().After(deadline) {
				c.Stop()
				b.Fatalf("commit timeout: committed=%d want=%d dropped=%d failed=%d", c.TotalCommitted(), level, c.TotalDropped(), c.TotalFailed())
			}
			runtime.Gosched()
		}
		c.Stop()
		b.StartTimer()
	}
	b.SetBytes(int64(level))
}

func BenchmarkHLFLoad100K(b *testing.B) { benchmarkHLFLoad(b, 100_000) }
func BenchmarkHLFLoad250K(b *testing.B) { benchmarkHLFLoad(b, 250_000) }
func BenchmarkHLFLoad500K(b *testing.B) { benchmarkHLFLoad(b, 500_000) }
func BenchmarkHLFLoad1M(b *testing.B)   { benchmarkHLFLoad(b, 1_000_000) }
func BenchmarkHLFLoad2M(b *testing.B)   { benchmarkHLFLoad(b, 2_000_000) }
func BenchmarkHLFLoad5M(b *testing.B)   { benchmarkHLFLoad(b, 5_000_000) }
