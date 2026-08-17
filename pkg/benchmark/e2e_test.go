package benchmark

import (
	"context"
	"testing"

	"hed-core/pkg/engine"
)

func TestRunE2EExactAccounting(t *testing.T) {
	p := engine.NewPipeline(nil, 16)
	const n = 10003
	r := RunE2E(context.Background(), p, func(context.Context, *engine.TxPayload) error { return nil }, n, 64)
	if r.Transactions != n { t.Fatalf("transactions=%d want %d", r.Transactions, n) }
	if r.Succeeded != n || r.Failed != 0 { t.Fatalf("succeeded=%d failed=%d", r.Succeeded, r.Failed) }
}
