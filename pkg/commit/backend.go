package commit

import "hed-core/pkg/engine"

type Backend interface {
	Commit([]*engine.TxPayload) error
}

type NoopBackend struct{}

func (NoopBackend) Commit(batch []*engine.TxPayload) error { return nil }
