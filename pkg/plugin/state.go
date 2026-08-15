package plugin

import (
	"fmt"
	"sync"
)

// StateEngine is the core storage contract used by HED's stateful transaction path.
// BatchWrite applies relative balance deltas. BatchWriteWithID additionally makes
// the logical batch replay-safe when the backend supports idempotency.
type StateEngine interface {
	Name() string
	Init(config map[string]string) error
	GetState(channelID string, key string) ([]byte, error)
	PutState(channelID string, key string, value []byte) error
	BatchWrite(channelID string, updates map[string]int64) error
	BatchWriteWithID(requestID, channelID string, updates map[string]int64) error
	Close() error
}

type Registry struct {
	mu sync.RWMutex
	plugins map[string]StateEngine
}

func NewRegistry() *Registry { return &Registry{plugins: make(map[string]StateEngine)} }

func (r *Registry) Register(engine StateEngine) {
	if engine == nil { return }
	r.mu.Lock(); r.plugins[engine.Name()] = engine; r.mu.Unlock()
}

func (r *Registry) Get(name string) (StateEngine, error) {
	r.mu.RLock(); engine, exists := r.plugins[name]; r.mu.RUnlock()
	if !exists { return nil, fmt.Errorf("plugin %s not found", name) }
	return engine, nil
}

func (r *Registry) List() []string {
	r.mu.RLock()
	names := make([]string, 0, len(r.plugins))
	for n := range r.plugins { names = append(names, n) }
	r.mu.RUnlock()
	return names
}
