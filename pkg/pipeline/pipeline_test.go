package pipeline

import (
	"testing"
	"time"
	"hed-core/pkg/engine"
)

func TestPipelineRoutesCommitsAndDeduplicates(t *testing.T) {
	p := New(Config{Partitions: 4, QueueSize: 64, BatchSize: 8}, nil)
	defer p.Stop()
	for i := 0; i < 100; i++ {
		if err := p.Submit(&engine.TxPayload{TxUUID: string(rune(i + 1)), AccountID: "acct", Amount: int64(i)}); err != nil { t.Fatal(err) }
	}
	if err := p.Submit(&engine.TxPayload{TxUUID: string(rune(1)), AccountID: "acct"}); err == nil { t.Fatal("duplicate accepted") }
	deadline := time.Now().Add(time.Second)
	for p.Metrics()["committed"] < 100 && time.Now().Before(deadline) { time.Sleep(time.Millisecond) }
	if got := p.Metrics()["committed"]; got != 100 { t.Fatalf("committed=%d", got) }
}
