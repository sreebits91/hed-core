package benchmark

import (
	"context"
	"testing"
	"time"

	"hed-core/pkg/delta"
)

func TestRun256WorkerBenchmarkReachesExactTarget(t *testing.T) {
	d := delta.New(nil)
	result := Run256WorkerBenchmark(context.Background(), d, 100_000, 256)
	if result.TotalTx != 100_000 {
		t.Fatalf("total tx=%d, want 100000", result.TotalTx)
	}
	if result.TPS <= 0 {
		t.Fatal("expected positive TPS")
	}
}

func TestRun256WorkerBenchmarkHonorsCancellation(t *testing.T) {
	d := delta.New(nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := Run256WorkerBenchmark(ctx, d, 100_000, 256)
	if result.TotalTx != 0 {
		t.Fatalf("cancelled benchmark committed %d transactions", result.TotalTx)
	}
}

func BenchmarkDeltaEngine100K(b *testing.B) {
	for i := 0; i < b.N; i++ {
		d := delta.New(nil)
		start := time.Now()
		result := Run256WorkerBenchmark(context.Background(), d, 100_000, 256)
		b.ReportMetric(float64(result.TotalTx)/time.Since(start).Seconds(), "tx/s")
	}
}
