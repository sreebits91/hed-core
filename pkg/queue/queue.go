package queue

import "hed-core/pkg/engine"

type TransactionQueue interface {
	Push(*engine.TxPayload) bool
	Pop() (*engine.TxPayload, bool)
	Len() int
	Capacity() int
	Close()
}
