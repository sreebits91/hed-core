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
// Submit must return only after the transaction's commit status is known.
type Client interface {
	Submit(context.Context, *engine.TxPayload) error
	Close() error
}

// ReadinessChecker is implemented by clients that can actively verify that
// Fabric is reachable and usable (peer/orderer/gateway health as appropriate).
type ReadinessChecker interface {
	Ready(context.Context) error
}

type LifecycleState uint32

const (
	StateStopped LifecycleState = iota
	StateStarting
	StateReady
	StateDegraded
)

func (s LifecycleState) String() string {
	switch s {
	case StateStarting:
		return "starting"
	case StateReady:
		return "ready"
	case StateDegraded:
		return "degraded"
	default:
		return "stopped"
	}
}

type FabricLifecycle struct {
	mu     sync.RWMutex
	client Client
	state  atomic.Uint32
	closed bool
}

func NewFabricLifecycle(client Client) (*FabricLifecycle, error) {
	if client == nil {
		return nil, errors.New("hlf: nil Fabric client")
	}
	f := &FabricLifecycle{client: client}
	// A constructed client is not proof that Fabric is usable. Readiness must
	// be established explicitly by CheckReady or by a successful committed tx.
	f.state.Store(uint32(StateStarting))
	return f, nil
}

func (f *FabricLifecycle) State() LifecycleState {
	return LifecycleState(f.state.Load())
}

// Ready reports the only state in which HED should advertise Fabric readiness.
func (f *FabricLifecycle) Ready() bool {
	return f.State() == StateReady
}

// CheckReady performs an explicit readiness probe when the underlying client
// supports it. Clients without a readiness contract cannot be declared ready.
func (f *FabricLifecycle) CheckReady(ctx context.Context) error {
	f.mu.RLock()
	client := f.client
	closed := f.closed
	f.mu.RUnlock()
	if closed || client == nil {
		return errors.New("hlf: Fabric lifecycle is stopped")
	}

	checker, ok := client.(ReadinessChecker)
	if !ok {
		f.state.Store(uint32(StateDegraded))
		return errors.New("hlf: Fabric client does not support readiness checks")
	}
	if err := checker.Ready(ctx); err != nil {
		f.state.Store(uint32(StateDegraded))
		return fmt.Errorf("hlf: Fabric readiness check: %w", err)
	}
	f.state.Store(uint32(StateReady))
	return nil
}

func (f *FabricLifecycle) Submit(ctx context.Context, tx *engine.TxPayload) error {
	if tx == nil {
		return errors.New("hlf: nil transaction")
	}
	f.mu.RLock()
	client := f.client
	closed := f.closed
	f.mu.RUnlock()
	if closed || client == nil {
		return errors.New("hlf: Fabric lifecycle is stopped")
	}

	if err := client.Submit(ctx, tx); err != nil {
		f.state.Store(uint32(StateDegraded))
		return fmt.Errorf("hlf: Fabric submit/commit: %w", err)
	}
	// A successful operation proves the gateway path worked for this tx.
	f.state.Store(uint32(StateReady))
	return nil
}

func (f *FabricLifecycle) Close() error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil
	}
	f.closed = true
	client := f.client
	f.client = nil
	f.state.Store(uint32(StateStopped))
	f.mu.Unlock()
	if client == nil {
		return nil
	}
	return client.Close()
}

func (f *FabricLifecycle) Reconnect(client Client) error {
	if client == nil {
		return errors.New("hlf: nil Fabric client")
	}
	f.mu.Lock()
	old := f.client
	f.client = client
	f.closed = false
	f.state.Store(uint32(StateStarting))
	f.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	return nil
}

// Commit adapts the lifecycle to HLFCommitter's CommitSink interface.
type LifecycleSink struct{ Lifecycle *FabricLifecycle }

func (s LifecycleSink) Commit(ctx context.Context, batch []*engine.TxPayload) error {
	if s.Lifecycle == nil {
		return errors.New("hlf: nil lifecycle")
	}
	for _, tx := range batch {
		if err := s.Lifecycle.Submit(ctx, tx); err != nil {
			return err
		}
	}
	return nil
}
