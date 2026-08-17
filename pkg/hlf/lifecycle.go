package hlf

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"hed-core/pkg/engine"
)

// Client is the minimal Fabric Gateway client contract required by HED.
// A concrete implementation can wrap github.com/hyperledger/fabric-gateway/v2.
// Submit must return only after the transaction's commit status is known.
type Client interface {
	Submit(context.Context, *engine.TxPayload) error
	Close() error
}

type LifecycleState uint32

const (
	StateStopped LifecycleState = iota
	StateReady
	StateDegraded
)

type FabricLifecycle struct {
	mu sync.RWMutex
	client Client
	state atomic.Uint32
	closed bool
}

func NewFabricLifecycle(client Client) (*FabricLifecycle, error) {
	if client == nil { return nil, errors.New("hlf: nil Fabric client") }
	f := &FabricLifecycle{client: client}
	f.state.Store(uint32(StateReady))
	return f, nil
}

func (f *FabricLifecycle) State() LifecycleState { return LifecycleState(f.state.Load()) }

func (f *FabricLifecycle) Submit(ctx context.Context, tx *engine.TxPayload) error {
	if tx == nil { return errors.New("hlf: nil transaction") }
	f.mu.RLock()
	client := f.client
	closed := f.closed
	f.mu.RUnlock()
	if closed || client == nil { return errors.New("hlf: Fabric lifecycle is stopped") }
	if err := client.Submit(ctx, tx); err != nil {
		f.state.Store(uint32(StateDegraded))
		return fmt.Errorf("hlf: Fabric submit/commit: %w", err)
	}
	f.state.Store(uint32(StateReady))
	return nil
}

func (f *FabricLifecycle) Close() error {
	f.mu.Lock()
	if f.closed { f.mu.Unlock(); return nil }
	f.closed = true
	client := f.client
	f.client = nil
	f.state.Store(uint32(StateStopped))
	f.mu.Unlock()
	if client == nil { return nil }
	return client.Close()
}

func (f *FabricLifecycle) Reconnect(client Client) error {
	if client == nil { return errors.New("hlf: nil Fabric client") }
	f.mu.Lock()
	old := f.client
	f.client = client
	f.closed = false
	f.state.Store(uint32(StateReady))
	f.mu.Unlock()
	if old != nil { _ = old.Close() }
	return nil
}

// Commit adapts the lifecycle to HLFCommitter's CommitSink interface.
type LifecycleSink struct { Lifecycle *FabricLifecycle }
func (s LifecycleSink) Commit(ctx context.Context, batch []*engine.TxPayload) error {
	if s.Lifecycle == nil { return errors.New("hlf: nil lifecycle") }
	for _, tx := range batch {
		if err := s.Lifecycle.Submit(ctx, tx); err != nil { return err }
	}
	return nil
}
