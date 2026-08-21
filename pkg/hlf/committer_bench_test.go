package hlf

import (
	"testing"

	"hed-core/pkg/engine"
)

func BenchmarkHLFCommitterSubmitTx(b *testing.B) {
	c := NewHLFCommitter(BatchConfig{MaxBatchSize: 2000, WorkerCount: 32, QueueSize: 1000000})
	defer c.Stop()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := &engine.TxPayload{TxUUID: "bench", AccountID: "acc", Amount: int64(i)}
		if !c.SubmitTx(tx) {
			b.Fatal("unexpected queue saturation")
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "tx/s")
}
