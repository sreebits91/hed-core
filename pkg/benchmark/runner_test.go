package benchmark

import (
	"context"
	"testing"

	"hed-core/pkg/delta"
)

func BenchmarkDeltaEngineWorkers(b *testing.B) {
	for _, workers := range []int{64, 128, 256, 512, 1024} {
		b.Run("workers="+itoa(workers), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				e := delta.New(nil)
				Run256WorkerBenchmark(context.Background(), e, 100000, workers)
			}
		})
	}
}

func TestRun256WorkerBenchmark(t *testing.T) {
	e := delta.New(nil)
	result := Run256WorkerBenchmark(context.Background(), e, 10000, 256)
	if result.TotalTx != 10000 {
		t.Fatalf("TotalTx = %d, want 10000", result.TotalTx)
	}
	if result.TPS <= 0 {
		t.Fatalf("TPS = %f, want > 0", result.TPS)
	}
}

func itoa(v int) string {
	if v == 64 { return "64" }
	if v == 128 { return "128" }
	if v == 256 { return "256" }
	if v == 512 { return "512" }
	return "1024"
}
