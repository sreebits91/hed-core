package queue

import (
	"testing"
	"hed-core/pkg/engine"
)

func TestRingQueueFIFOAndCapacity(t *testing.T) {
	q := NewRingQueue(2)
	a := &engine.TxPayload{TxUUID: "a"}
	b := &engine.TxPayload{TxUUID: "b"}
	if !q.Push(a) || !q.Push(b) { t.Fatal("expected pushes to succeed") }
	if q.Push(&engine.TxPayload{TxUUID: "c"}) { t.Fatal("expected full queue rejection") }
	x, ok := q.Pop(); if !ok || x != a { t.Fatal("queue is not FIFO") }
	if !q.Push(&engine.TxPayload{TxUUID: "c"}) { t.Fatal("expected wraparound push") }
	if q.Len() != 2 { t.Fatalf("unexpected length: %d", q.Len()) }
	q.Close()
	if q.Push(&engine.TxPayload{TxUUID: "d"}) { t.Fatal("closed queue accepted push") }
}
