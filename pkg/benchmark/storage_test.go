package benchmark

import (
	"context"
	"sync"
	"testing"
)

type memoryStateEngine struct {
	mu    sync.Mutex
	items int
}

func (m *memoryStateEngine) Name() string { return "memory-test" }
func (m *memoryStateEngine) Init(map[string]string) error { return nil }
func (m *memoryStateEngine) GetState(string, string) ([]byte, error) { return nil, nil }
func (m *memoryStateEngine) PutState(string, string, []byte) error { return nil }
func (m *memoryStateEngine) BatchWrite(_ string, updates map[string][]byte) error {
	m.mu.Lock()
	m.items += len(updates)
	m.mu.Unlock()
	return nil
}
func (m *memoryStateEngine) Close() error { return nil }

func TestRunStorageBenchmarkAccountsEverySuccessfulTransaction(t *testing.T) {
	db := &memoryStateEngine{}
	result := RunStorageBenchmark(context.Background(), db, 10000, 32, 25)
	if result.Transactions != 10000 {
		t.Fatalf("transactions=%d, want 10000", result.Transactions)
	}
	if result.ErrorCount != 0 {
		t.Fatalf("errors=%d, want 0", result.ErrorCount)
	}
	if db.items != 10000 {
		t.Fatalf("persisted=%d, want 10000", db.items)
	}
	if result.TPS <= 0 {
		t.Fatal("expected positive TPS")
	}
}
