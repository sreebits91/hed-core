package delta

import (
	"errors"
	"sync"
	"testing"

	"hed-core/pkg/plugin"
)

type mockState struct {
	mu     sync.Mutex
	writes []string
	fail   bool
}

func (m *mockState) Name() string                              { return "mock" }
func (m *mockState) Init(map[string]string) error              { return nil }
func (m *mockState) GetState(string, string) ([]byte, error)   { return nil, nil }
func (m *mockState) PutState(string, string, []byte) error     { return nil }
func (m *mockState) BatchWrite(string, map[string]int64) error { return nil }
func (m *mockState) BatchWriteWithID(requestID, channel string, updates map[string]int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail {
		return errors.New("temporary failure")
	}
	m.writes = append(m.writes, requestID)
	return nil
}
func (m *mockState) Close() error { return nil }

var _ plugin.StateEngine = (*mockState)(nil)

func TestFlushRetriesFailedBatchWithSameRequestID(t *testing.T) {
	store := &mockState{fail: true}
	d := New(store)
	d.ApplyDelta("channel1", "alice", 100)
	if err := d.FlushToDB("channel1"); err == nil {
		t.Fatal("expected persistence failure")
	}

	store.fail = false
	if err := d.FlushToDB("channel1"); err != nil {
		t.Fatal(err)
	}
	if len(store.writes) != 1 {
		t.Fatalf("expected one successful logical write, got %d", len(store.writes))
	}
}

func TestChannelIsolation(t *testing.T) {
	store := &mockState{}
	d := New(store)
	d.ApplyDelta("a", "same", 10)
	d.ApplyDelta("b", "same", 20)
	if err := d.FlushToDB("a"); err != nil {
		t.Fatal(err)
	}
	if len(store.writes) != 1 {
		t.Fatalf("expected only channel a to flush, got %d", len(store.writes))
	}
	if err := d.FlushToDB("b"); err != nil {
		t.Fatal(err)
	}
	if len(store.writes) != 2 {
		t.Fatalf("expected channel b to remain pending, got %d writes", len(store.writes))
	}
}
