package hlf

import (
	"context"

	"hed-core/pkg/engine"
)

// Gateway is the narrow boundary between HED and a real Hyperledger Fabric
// client. A production implementation must not report success until the
// required Fabric commit event has been observed.
type Gateway interface {
	Submit(context.Context, *engine.TxPayload) error
	Close() error
}

// CommitterBackend adapts a Gateway to a batching caller.
type CommitterBackend struct { Gateway Gateway }

func (b CommitterBackend) Commit(ctx context.Context, batch []*engine.TxPayload) error {
	if b.Gateway == nil { return nil }
	for _, tx := range batch {
		if err := b.Gateway.Submit(ctx, tx); err != nil { return err }
	}
	return nil
}
