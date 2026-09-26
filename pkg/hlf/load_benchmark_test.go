package hlf

import (
	"os"
	"runtime"
	"strconv"
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
		Partitions:   32,
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

	timeout := 30 * time.Second
	if n >= 1_000_000 { timeout = 2 * time.Minute }
	deadline := time.Now().Add(timeout)
	for c.TotalCommitted() < uint64(n) {
		if time.Now().After(deadline) {
			t.Fatalf("commit timeout: committed=%d want=%d dropped=%d failed=%d", c.TotalCommitted(), n, c.TotalDropped(), c.TotalFailed())
		}
		runtime.Gosched()
	}
}

func TestHLFLoadLevels(t *testing.T) {
	if testing.Short() { t.Skip("load qualification skipped in short mode") }
	levels := []int{100_000, 250_000, 500_000}
	if os.Getenv("HED_PERF_LEVELS") == "1" { levels = []int{100_000, 250_000, 500_000, 1_000_000, 2_700_000, 5_000_000} }
	if os.Getenv("HED_PERF_LEVELS") == "2" { levels = []int{2_700_000} }
	for _, n := range levels {
		t.Run(loadLevelName(n), func(t *testing.T) {
			c := newBenchmarkCommitter()
			defer c.Stop()
			start := time.Now()
			submitLoad(t, c, n)
			elapsed := time.Since(start)
			tps := float64(n) / elapsed.Seconds()
			t.Logf("load=%d elapsed=%s tps=%.0f", n, elapsed, tps)
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
		return strconv.FormatInt(int64(n), 10)
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
		timeout := 30 * time.Second
		if level >= 1_000_000 { timeout = 2 * time.Minute }
		deadline := time.Now().Add(timeout)
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
func BenchmarkHLFLoad2_7M(b *testing.B) { benchmarkHLFLoad(b, 2_700_000) }
func BenchmarkHLFLoad5M(b *testing.B)   { benchmarkHLFLoad(b, 5_000_000) }
