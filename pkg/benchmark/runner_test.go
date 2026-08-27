package benchmark

import (
	"context"
	"testing"

	"hed-core/pkg/delta"
)

func BenchmarkRun256Workers(b *testing.B) {
	for i := 0; i < b.N; i++ {
		engine := delta.NewDeltaEngine()
		result := Run256WorkerBenchmark(context.Background(), engine, 2560, 256)
		if result.TotalTx != 2560 {
			b.Fatalf("committed transactions = %d, want 2560", result.TotalTx)
		}
	}
}

func TestRun256WorkerBenchmark(t *testing.T) {
	engine := delta.NewDeltaEngine()
	result := Run256WorkerBenchmark(context.Background(), engine, 2560, 256)
	if result.TotalTx != 2560 {
		t.Fatalf("committed transactions = %d, want 2560", result.TotalTx)
	}
	if result.TPS <= 0 {
		t.Fatalf("TPS = %v, want > 0", result.TPS)
	}
}
