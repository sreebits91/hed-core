package hlf

import (
	"context"
	"errors"
	"testing"

	"hed-core/pkg/engine"
)

type fakeClient struct{ submits, closes int; err error }
func (f *fakeClient) Submit(context.Context,*engine.TxPayload) error { f.submits++; return f.err }
func (f *fakeClient) Close() error { f.closes++; return nil }

func TestFabricLifecycleCommitState(t *testing.T) {
	c:=&fakeClient{}
	l,err:=NewFabricLifecycle(c); if err!=nil {t.Fatal(err)}
	if l.State()!=StateReady {t.Fatal("expected ready")}
	if err:=l.Submit(context.Background(), &engine.TxPayload{}); err!=nil {t.Fatal(err)}
	if c.submits!=1 {t.Fatalf("submits=%d",c.submits)}
	if err:=l.Close(); err!=nil {t.Fatal(err)}
	if l.State()!=StateStopped {t.Fatal("expected stopped")}
}

func TestFabricLifecycleMarksDegradedOnCommitFailure(t *testing.T) {
	c:=&fakeClient{err:errors.New("commit failed")}
	l,err:=NewFabricLifecycle(c); if err!=nil {t.Fatal(err)}
	if err:=l.Submit(context.Background(), &engine.TxPayload{}); err==nil {t.Fatal("expected error")}
	if l.State()!=StateDegraded {t.Fatal("expected degraded")}
}
