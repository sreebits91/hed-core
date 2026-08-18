package hlf

import (
	"context"
	"errors"
	"testing"

	"hed-core/pkg/engine"
)

type fakeClient struct {
	submits, closes int
	err              error
	readyErr         error
}

func (f *fakeClient) Submit(context.Context, *engine.TxPayload) error {
	f.submits++
	return f.err
}
func (f *fakeClient) Close() error { f.closes++; return nil }
func (f *fakeClient) Ready(context.Context) error { return f.readyErr }

func TestFabricLifecycleRequiresExplicitReadiness(t *testing.T) {
	c := &fakeClient{}
	l, err := NewFabricLifecycle(c)
	if err != nil {
		t.Fatal(err)
	}
	if l.State() != StateStarting || l.Ready() {
		t.Fatalf("initial state=%s, want starting", l.State())
	}
	if err := l.CheckReady(context.Background()); err != nil {
		t.Fatal(err)
	}
	if l.State() != StateReady || !l.Ready() {
		t.Fatalf("state=%s, want ready", l.State())
	}
	if err := l.Submit(context.Background(), &engine.TxPayload{}); err != nil {
		t.Fatal(err)
	}
	if c.submits != 1 {
		t.Fatalf("submits=%d", c.submits)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if l.State() != StateStopped {
		t.Fatal("expected stopped")
	}
}

func TestFabricLifecycleReadinessFailure(t *testing.T) {
	c := &fakeClient{readyErr: errors.New("peer unavailable")}
	l, err := NewFabricLifecycle(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.CheckReady(context.Background()); err == nil {
		t.Fatal("expected readiness error")
	}
	if l.State() != StateDegraded || l.Ready() {
		t.Fatalf("state=%s, want degraded", l.State())
	}
}

func TestFabricLifecycleMarksDegradedOnCommitFailure(t *testing.T) {
	c := &fakeClient{err: errors.New("commit failed")}
	l, err := NewFabricLifecycle(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.CheckReady(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := l.Submit(context.Background(), &engine.TxPayload{}); err == nil {
		t.Fatal("expected error")
	}
	if l.State() != StateDegraded {
		t.Fatal("expected degraded")
	}
}

func TestFabricLifecycleReconnectReturnsToStarting(t *testing.T) {
	first := &fakeClient{}
	second := &fakeClient{}
	l, err := NewFabricLifecycle(first)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.CheckReady(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := l.Reconnect(second); err != nil {
		t.Fatal(err)
	}
	if l.State() != StateStarting || l.Ready() {
		t.Fatalf("state=%s, want starting", l.State())
	}
	if first.closes != 1 {
		t.Fatalf("old client closes=%d, want 1", first.closes)
	}
}
