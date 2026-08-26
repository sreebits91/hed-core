package partition

import (
	"testing"
	"hed-core/pkg/engine"
	"hed-core/pkg/queue"
)

func TestRouterDeterministic(t *testing.T) {
	qs := []queue.TransactionQueue{queue.NewRingQueue(8), queue.NewRingQueue(8), queue.NewRingQueue(8), queue.NewRingQueue(8)}
	r := New(qs)
	tx := &engine.TxPayload{TxUUID: "stable-id"}
	if !r.Route(tx) { t.Fatal("route failed") }
	if r.PartitionCount() != 4 { t.Fatal("unexpected partition count") }
	found := 0
	for _, q := range qs { found += q.Len() }
	if found != 1 { t.Fatalf("expected one routed transaction, got %d", found) }
}
