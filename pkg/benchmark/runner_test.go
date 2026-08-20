package benchmark

import (
	"context"
	"testing"

	"hed-core/pkg/delta"
)

func BenchmarkRun256WorkerBenchmark(b *testing.B) {
	for i := 0; i < b.N; i++ {
		e := delta.New(nil)
		Run256WorkerBenchmark(context.Background(), e, 10000, 256)
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
